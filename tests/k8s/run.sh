#!/usr/bin/env bash
#
# Integration test for HSDS running headless in Kubernetes (k3s via k3d).
#
# Guards against the failure mode where service nodes never leave WAITING and
# every request returns 503. Neither unit tests nor `kubectl get pods` catch it:
# containers stay Running and their liveness probes stay green, because /info
# bypasses the node_state gate (see INFO_METHODS in hsds_logger.request).
# Detecting it needs a real cluster, an explicit readiness assertion, and a
# request against a *gated* route.
#
# The scale-up step is not incidental. Readiness bugs here surface as races
# during rescale - a data node reporting node_number -1, or two nodes briefly
# claiming the same number - so a single-replica test can pass while a
# multi-replica deployment wedges.
#
# Readiness is asserted per *container*, not per pod: sn and dn run separate
# health checks and converge independently, so an sn can report READY while its
# own dn is still WAITING. Requests that shard to that dn then 503.
#
# Usage:
#   tests/k8s/run.sh [replicas]     # default 3
#   KEEP=1 tests/k8s/run.sh         # leave the cluster up for debugging
#
# Requires: docker, k3d, kubectl.

set -euo pipefail

CLUSTER=hsds-test
NS=hsds-test
IMAGE=hsds-test:ci
REPLICAS="${1:-3}"
READY_TIMEOUT="${READY_TIMEOUT:-180}"
# enough requests that round-robin reaches every replica several times
SERVICE_PROBES="${SERVICE_PROBES:-12}"
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"

# Keep this cluster out of ~/.kube/config entirely. Without this, k3d switches the
# caller's current context to the test cluster, and an aborted run (or KEEP=1)
# leaves it there - so a later kubectl in the same shell silently targets the
# wrong cluster. Anyone running this on a machine that also has production
# credentials should not have their context touched by a test.
export KUBECONFIG="${TMPDIR:-/tmp}/kubeconfig-${CLUSTER}"

for tool in docker k3d kubectl; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "FATAL: '$tool' not found. Install it (brew install $tool) and retry." >&2
    exit 1
  }
done

cleanup() {
  if [ "${KEEP:-0}" = "1" ]; then
    echo
    echo "KEEP=1 - leaving cluster '$CLUSTER' up. Inspect with:"
    echo "  kubectl -n $NS get pods"
    echo "Delete with: k3d cluster delete $CLUSTER"
  else
    echo
    echo "--> tearing down cluster '$CLUSTER'"
    k3d cluster delete "$CLUSTER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

pods() {
  kubectl -n "$NS" get pods -l app=hsds \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null
}

# node_state of one container in a pod, using the same hsds-node-state script the
# probes run. It prints the state and derives its port from NODE_TYPE, so no port
# needs threading through here.
#
# `|| true` because it exits non-zero when the node is not READY - correct for a
# probe, but here we only want the printed state and pipefail would otherwise make
# a WAITING node indistinguishable from a failed kubectl.
node_state() { # $1=pod  $2=container
  kubectl -n "$NS" exec "$1" -c "$2" -- hsds-node-state 2>/dev/null | tr -d '\r\n' || true
}

# A pod only serves once BOTH its nodes are READY. sn and dn each run their own
# health check and converge independently, so an sn can report READY while its dn
# is still WAITING - and a request that shards to that dn gets a 503 from the dn's
# own gate. Asserting on sn alone lets this test pass while the cluster is not
# actually serving, which is exactly how it produced a false PASS.
pod_ready() { # $1=pod
  [ "$(node_state "$1" sn)" = "READY" ] &&
    [ "$(node_state "$1" dn)" = "READY" ]
}

# Status counts for repeated GETs against a *gated* route, via the ClusterIP
# service. A missing domain gives 404; 503 means a node was not ready, which is the
# bug this test exists to catch.
#
# Sends many requests rather than one: the service load-balances, so a single
# request only ever exercises one replica and can pass while the others 503.
service_status_counts() {
  local pod="$1"
  kubectl -n "$NS" exec "$pod" -c sn -- python -c "
import collections, urllib.request, urllib.error, urllib.parse
url = 'http://hsds.${NS}.svc.cluster.local/?domain=' + urllib.parse.quote('/home/nosuch.h5')
counts = collections.Counter()
for _ in range(${SERVICE_PROBES}):
    try:
        counts[urllib.request.urlopen(url).status] += 1
    except urllib.error.HTTPError as e:
        counts[e.code] += 1
    except Exception as e:
        counts[type(e).__name__] += 1
print(' '.join(f'{k}={v}' for k, v in sorted(counts.items(), key=str)))
" 2>/dev/null | tr -d '\r\n'
}

diagnose() {
  echo
  echo "===== diagnostics ====="
  kubectl -n "$NS" get pods -o wide 2>&1 || true
  echo "--- per-container node_state (sn and dn converge independently) ---"
  for p in $(pods); do
    echo "    $p  sn=$(node_state "$p" sn)  dn=$(node_state "$p" dn)"
  done
  for p in $(pods); do
    for c in sn dn; do
      echo "--- $p ($c) readiness-relevant log lines ---"
      kubectl -n "$NS" logs "$p" -c "$c" --tail=60 2>&1 |
        grep -E "node_state|cluster_state|scaling|dn_urls|not consecutive|converged" || true
    done
  done
}

assert_all_ready() {
  local want="$1" waited=0 ready count p
  echo "--> waiting for $want pod(s) with BOTH sn and dn READY (timeout ${READY_TIMEOUT}s)"
  while [ "$waited" -lt "$READY_TIMEOUT" ]; do
    count=0
    ready=0
    for p in $(pods); do
      count=$((count + 1))
      pod_ready "$p" && ready=$((ready + 1))
    done
    if [ "$count" -eq "$want" ] && [ "$ready" -eq "$want" ]; then
      echo "    PASS: $ready/$want READY"
      return 0
    fi
    echo "    $ready/$count READY, waiting..."
    sleep 5
    waited=$((waited + 5))
  done
  echo "    FAIL: only $ready/$want reached READY within ${READY_TIMEOUT}s" >&2
  diagnose
  return 1
}

assert_serving() {
  local pod counts
  pod="$(pods | head -1)"
  counts="$(service_status_counts "$pod")"
  if [ -z "$counts" ]; then
    echo "    FAIL: no response from service" >&2
    diagnose
    return 1
  fi
  case "$counts" in
    *503*)
      echo "    FAIL: gated route returned 503 - nodes are not serving: $counts" >&2
      diagnose
      return 1
      ;;
  esac
  # anything that is not an HTTP status (an exception class name) is also a failure
  case "$counts" in
    *[A-Za-z]=*)
      echo "    FAIL: request errors against service: $counts" >&2
      diagnose
      return 1
      ;;
  esac
  echo "    PASS: ${SERVICE_PROBES} requests via service, no 503 -> $counts"
}

echo "==> building $IMAGE"
docker build -q -t "$IMAGE" "$REPO" >/dev/null

echo "==> creating k3d cluster '$CLUSTER'"
k3d cluster delete "$CLUSTER" >/dev/null 2>&1 || true
k3d cluster create "$CLUSTER" --wait \
  --kubeconfig-update-default=false \
  --kubeconfig-switch-context=false >/dev/null
k3d kubeconfig get "$CLUSTER" >"$KUBECONFIG"

echo "==> importing $IMAGE into the cluster"
k3d image import "$IMAGE" -c "$CLUSTER" >/dev/null

echo "==> applying manifests"
kubectl create namespace "$NS" >/dev/null
kubectl -n "$NS" apply -f "$HERE/manifests.yaml" >/dev/null
# Not fatal: the readiness probe means a broken build never reports Ready, so this
# would abort under `set -e` before the assertions below run - and their output is the
# whole point, since a bare "timed out waiting for the condition" says nothing about
# why. Let it fail and let assert_all_ready produce the diagnostics.
kubectl -n "$NS" rollout status deploy/hsds --timeout=180s || true

echo
echo "== single replica =="
assert_all_ready 1
assert_serving

echo
echo "== scaled to $REPLICAS replicas =="
kubectl -n "$NS" scale deploy/hsds --replicas="$REPLICAS" >/dev/null
kubectl -n "$NS" rollout status deploy/hsds --timeout=300s || true  # see above
assert_all_ready "$REPLICAS"
assert_serving

echo
echo "ALL CHECKS PASSED"
