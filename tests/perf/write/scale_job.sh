kubectl  patch job hsds-write-test -p '{"spec":{"parallelism":'${1}'}}'
