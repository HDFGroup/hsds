HSDS for AWS Lambda
===================

AWS Lambda is a technology that enables code to be run without the need to provision a server.  For AWS deployments, HSDS can be deployed as AWS Lambda function to provide more scalability and parallelism than would be practical compared with containers running in Docker or Kubernetes (AWS Lambda supports up to 1000-way parallelism by default).  

Each Lambda invocation will be charged based on how long the code took to execute (typically 2-4 seconds per request) and memory used (can be configured to anything between 128MB and 10GB). This is especially attractive for deployments were the service will be used intermittantly, as there is no charge unless the Lambda function is invoked.

Compared with a traiditional deployment, a Lambda deployment is not optimal for situtaions where the lowest possible latency is desired.  Since the Lambda function takes a certain amount of time to "spin up",
the average latency will be higher compared to a lightly loaded server deployment.  On the other hand, Lambda invocations generally have a more consistent latency.  Even with a high request rate, the latency should be the same or lower since there is no contention among the executing lambda functions.

Finally, Lambda is best used for read-only applications.  In traditional deployments, HSDS ensures that POST and PUT requests are applied consistently.  If multiple update requests are sent simultaneously with Lambda it is possible to have a race condition where some udpates will get overwritten.

Function Creation
=================

To use HSDS for Lambda, follow these steps:

1. In the AWS Management Console, go to Elastic Container Registry (ECR) and create a repository to store the Lambda container image.
2. Download the latest image from https://gallery.ecr.aws/w7l0z8b2/hdfgroup with a tag starting in "hslambda".  E.g. `$ docker pull public.ecr.aws/w7l0z8b2/hdfgroup:hslambda_v0.7.0beta01`.  Alternatively, you can build the Lambda image from source, see: "build HSDS Lambda"
3. Go to the respository you created in ECR and follow the "View push commands" instructions to deploy the Lambda image to your repository
4. Select the Lambda service in the Management Console
5. Click the "Create Function" button
6. Choose the "Container Image" option
7. Enter a function name (e.g. "hslambda")
8. Click the "Browse Image" button and select the image you uploaded to ECR.  Wait for the image to load
9. Click "Create Function". Wait for image to load
10. Select the "Configuration" tab and change the memory value to at least 1024MB and Timeout of 30 sec (later try out different values for these to see which works best for your workload)
11. Select the "Test" tab and press the "Test" button using the default event
12. Function should succeed returning a JSON response with "status_code" of 200, and an "output" value containing general information about HSDS.  If a different status_code is returned, review the Log output to determine the nature of the error


Setting Permissions
===================

By default the Lambda function does not have permissions to access any S3 content.  To enable this, do the following:

1. In the "Configuration" tab, select "Environment variables", click the "Edit" button, and then the "Add environment variable" button.  Enter a key of "AWS_S3_GATEWAY" and a value corresponding to the S3 endpoint for your region.  E.g. "http://s3.us-west-2.amazonaws.com" for us-west-2
2. Next select "Permissions" and click the "Edit" button for "Execution Role".  Select (or create) a role that includes at least the policies: "AWSLambdaBasicExecutionRole" and "AmazonS3ReadOnlyAccess".  If desired, you may use "AmazonS3FullAccess" (for read-write applications), and/or restrict the resource to a given S3 bucket
3. Create a test event with the following values:

    {
      "method": "GET",
      "request": "/datasets/d-d29fda32-85f3-11e7-bf89-0242ac110008/value",
      "params": {
        "domain": "/nrel/wtk-us.h5",
        "select": "[0:100,620,1401]",
        "bucket": "nrel-pds-hsds"
      }
    }

The Execution result should now return a status_code 200, and output with a 100 data values.

General Usage
=============

HSDS for AWS Lambda supports the complete REST API supported by HSDS running as a service.  Since AWS Lambda currently doesn't support HTTP requests, it's necessary to "package" the components of a typical 
http request into the Lambda event structure.  Each event sent to the Lambda function should have key values of "method", "request", and "params" as explained below:

"method": one of the values "GET", "PUT", "POST", "DELETE" corresponding to the typical http verbs.
 
"request": the api to invoke.  This is the part of the url that would normally be placed after the http endpoint.

"params": a dictionary of query params to be sent to the function

See: https://github.com/HDFGroup/hdf-rest-api, for a complete description of the HDF REST API.

Building the Lambda Image
=========================

If you wish to build the Lambda image from source, clone this repository and run the script: "lambda_build.sh".  This will create a docker image that you can then push to ECR.

