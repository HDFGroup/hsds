HSDS for AWS Lambda
===================

AWS Lambda is a technology that enables code to be run without the need
to provision a server. For AWS deployments, HSDS can be deployed as a
AWS Lambda function to provide more scalability and parallelism than
would be practical compared with containers running in Docker or
Kubernetes (AWS Lambda supports up to 1000-way parallelism by default).

Each Lambda invocation will be charged based on how long the code took
to execute (typically 2-4 seconds per request) and memory used (can be
configured to anything between 1G and 10GB). This is especially
attractive for deployments were the service will be used intermittantly,
as there is no charge unless the Lambda function is invoked.

Compared with a traiditional deployment, a Lambda deployment is not
optimal for situtaions where the lowest possible latency is desired.
Since the Lambda function takes a certain amount of time to “spin up”,
the average latency will be higher compared to a lightly loaded server
deployment. On the other hand, Lambda invocations generally have a more
consistent latency. Even with a high request rate, the latency should be
the same or lower since there is little contention among the executing
Lambda functions.

HSDS for AWS Lambda supports the full HDF Rest API, though some care
should be taken if Lambda is used for operations that modify storage. If
multiple update requests are sent simultaneously with Lambda it is
possible to have a race condition where some udpates will get
overwritten. For example, if two Lambda functions are invoked
simultaneouly, and attempt to modify the same chunk, it is possible that
one function will overwrite the results of the other.

Function Creation
=================

To use HSDS for Lambda, follow these steps:

1.  In the AWS Management Console, select the Lambda service for your
    desired region
2.  Click the “Create Function” button
3.  Choose the “Container Image” option
4.  Enter a function name (e.g. “hslambda”)
5.  Click the “Browse Image” button and select the image you uploaded to
    ECR. Use one of the images “hslambda…” image tags in
    https://gallery.ecr.aws/w7l0z8b2/hdfgroup, or upload your own image
    to ECR. For the later, refer to “Building Lambda Images” below
6.  Wait for the image to load
7.  Click “Create Function”. Wait for image to load
8.  Select the “Configuration” tab and change the memory value to at
    least 1024MB and Timeout of 30 sec (later try out different values
    for these to see which works best for your workload)
9.  Also in the “Configuration” tab, select “Environment variables”,
    click the “Edit” button, and then the “Add environment variable”
    button. Enter a key of “AWS_S3_GATEWAY” and a value corresponding to
    the S3 endpoint for your region. E.g.
    “http://s3.us-west-2.amazonaws.com” for us-west-2
10. Next select “Permissions” and click the “Edit” button for “Execution
    Role”. Select (or create) a role that includes at least the
    policies: “AWSLambdaBasicExecutionRole” and
    “AmazonS3ReadOnlyAccess”. If desired, you may use
    “AmazonS3FullAccess” (for read-write applications), and/or restrict
    the resource to a given S3 bucket
11. Select the “Test” tab and press the “Test” button using the default
    event
12. Function should succeed returning a JSON response with “status_code”
    of 200, and an “output” value containing general information about
    HSDS. If a different status_code is returned, review the Log output
    to determine the nature of the error

General Usage
=============

HSDS for AWS Lambda supports the complete REST API supported by HSDS
running as a service. Since AWS Lambda currently doesn’t support HTTP
requests, it’s necessary to “package” the components of a typical http
request into the Lambda event structure. Each event sent to the Lambda
function should have key values of “method”, “request”, and “params” as
explained below:

-  “method”: one of the values “GET”, “PUT”, “POST”, “DELETE”
   corresponding to the typical http verbs.
-  “request”: the api to invoke. This is the part of the url that would
   normally be placed after the http endpoint.
-  “params”: a dictionary of query params to be sent to the function

The following is an example of how a hyperslab selection is packaged
into an event:

::

   {
     "method": "GET",
     "path": "/datasets/d-d29fda32-85f3-11e7-bf89-0242ac110008/value",
     "headers": {
       "accept": "application/octet-stream"
     },
     "params": {
       "domain": "/nrel/wtk-us.h5",
       "select": "[0:100,620,1401]",
       "bucket": "nrel-pds-hsds"
     }
   }

When the accept header with octet-stream is used with dataset value
requests, the body of the response will be hex-encoded (otherwise JSON
data is used).

With the request above, the following response should be returned:

::

   {
     "isBase64Encoded": true,
     "statusCode": 200,
     "headers": "{\"Content-Type\": \"application/octet-stream\", \"Content-Length\":    \"4000\", \"Date\": \"Fri, 16 Sep 2022 03:19:21 GMT\", \"Server\": \"Python/3.9 aiohttp/3.  8.1\"}",
     "body": "a8bb1241d0f7...e0bf6b40"
   }

Where the “body” key will consist of 8000 hex characters.

See: https://github.com/HDFGroup/hdf-rest-api, for a complete
description of the HDF REST API.

Access Control
==============

On execution the Lambda function will act using the Lambda function name
as username. Publicly readable domains (a “Default” ACL with read
permission set) will be accessible via the Lambda function without any
further action. To enable the Lambda function to read non-publicly
readable domains, use the hsacl tool to add read permission with the
Lambda function name as username.

For example, if the Lambda function is named “hslambda”, and the domain
is “/shared/data.h5”, run:

::

   hsacl /shared/data.h5 +r hslambda

Note: you’ll need to at least briefly run a Docker or Kubernetes-based
version of HSDS to run the above command.

Similarly, if the Lambda function will be modifying data, add
permissions to the Lambda user for update, write, or delete as needed.
The following would give the Lambda function full control of the domain
(other than reading or modifying ACLs):

::

   hsacl /shared/data.h5 +crud hslambda

Building the Lambda Image
=========================

If you wish to build the Lambda image from source, clone this repository
and run the script: “lambda_build.sh”. This will create a docker image
that you can then push to ECR.

API Gateway
===========

Amazon API Gateway is the service for managing HTTP endpoints. By
forwarding http requests to AWS Lambda, API Gateway can be used to
provide web server functionality without the need to provision a server
instance. When API Gateway is used with the HSDS Lambda function, the
functionality of HSDS will be available for HSDS clients such as h5pyd
and HDF REST VOL (to the clients it will be the same as connecting with
a regular HSDS instance).

To setup API Gateway with HSDS Lambda, follow these steps:

1.  In the AWS Management Console, select the API Gateway service for
    you desired region
2.  Click the “Create API” button
3.  Click the “Build” button in the “HTTP API” type box
4.  Click the “Add Integration” button
5.  In the drop down, choose “Lambda”
6.  Leave the “Version” as 2.0
7.  In the “Lambda function” box, chose the HSDS Lambda function you
    created earlier and click the “Add Integration” button
8.  Enter an API name and click “Next”
9.  In the “Configure routes” page, use “$default” for the “ANY” method,
    click “Next”
10. In the “Configure stages”, accept the defaults, click “Next”
11. In the “Review and Create” page, click “Create”
12. Upon successful creation of the gateway, you will see a “Invoke
    URL”. Entering that url in a browser and adding “/about” should
    return the contents of the about method
13. The same “Invoke URL” can be used as the endpoint for HSDS clients.
    By default that expected username will be the name of the lamba
    function and that password will be “lambda”

Using h5pyd with HSDS Lambda
============================

You can use h5pyd (version 0.10.4 and higher) to invoke HSDS Lambda
directly (i.e. no need to setup API Gateway). To use, install the Lambda
function as described above. Then use the following values for endpoint,
username, and password:

::

   hs_endpoint = http+lambda://hslambda
   hs_username = hslambda
   hs_password = lambda

If you named your Lambda function something different than “hslambda”,
replace hslambda with the function name. Make sure the following
environment variables are set as appropriatte for you AWS account and
region (“us-west-2” is used below) in which the Lambda function is
installed:

::

   export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_ID
   export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
   export AWS_REGION=us-west-2
   export AWS_LAMBDA_GATEWAY=https://lambda.us-west-2.amazonaws.com

Now you can use h5pyd in the same was as you would with an HSDS server
running. Each REST request that h5pyd would normally send to the server
will be replaced by a Lambda invocation. Depending on the program, the
can be quite a bit slower than running with a server (each Lambda
invocation will take 2 seconds or so).

Use the AWS Management Console to monitor Lambda usage and view logs as
needed.
