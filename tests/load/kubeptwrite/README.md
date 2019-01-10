# Load test - single element write

This test uses Kubernetes to have multiple clients writing to a single dataset.  Each
client writes one element per request for RUN_COUNT times (default is 1000).  This test assume a Kubernetes cluster has
already been setup and the user has kubectl configured for use with the cluster.


# Development

Test case code is rand_write.py, domain setup code is in setup.py


## Building

To build a new image, change into the directory of this README and run:
```
./build.sh
```
This will build an image called `hsclient/load-kubeptwrite`.


## Tagging

To push your image to the AWS registry it has to follow a strict tagging
scheme. Note that the usual name "tag" in docker lingo is a bit overloaded
here.

Find the image ID of the container image you want to tag with `docker images`.
Assuming that the ID of the container you want to tag is 82126fcb0658
run the following after looking up the current tag (AWS speak) used by a single
user image (visit https://us-west-2.console.aws.amazon.com/ecs/home?region=us-west-2#/repositories/hsclient#images;tagStatus=ALL and look for images tagged as `load-kubeoptwirte`).  Change XX below to a numeric revision.
```
docker tag 82126fcb0658 431396205827.dkr.ecr.us-west-2.amazonaws.com/hsclient:load-kubeptwrite_vXX
```

## Pushing to AWS container registry

Obtain the credentials to login to the AWS container registry:

```
aws ecr get-login --no-include-email
```

This will print a command starting with `docker login -u AWS ...`, run it.

To push the image you tagged in the previous step run:
```
docker push 431396205827.dkr.ecr.us-west-2.amazonaws.com/hsclient:load-kubeptwrite_vXX
```

## Running

Run ``python setup.py [domain] [extent]`` to initialize the domain.

Create a Kubernetes secret (see https://kubernetes.io/docs/concepts/configuration/secret/) for the HSDS password and adjust run.yaml appropriately. 

Adjust parameters in run.yaml as desired.  THe keys completions and parallism are used to control how many containers are run simultaneously.  Increase the size of the cluster as needed if the number of containers will be larged.

## Monitoring

Use ``viewjob.sh`` and ``getpods.sh`` to track progress of the job.  ``kubectl logs load-kubeptwrite-abcde`` to view the container log (where abcde is the last container name suffix as displayed in getpods.sh)

## Cleaning up

Run ``./killjob.sh`` to remove the job. ``hsrm [domain]`` can be used to remove the domain.
