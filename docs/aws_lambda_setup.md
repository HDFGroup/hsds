AWS Lambda Setup
================

AWS Lambda is a technology that enables code to be run without the need to provision a server.  For AWS deployments, HSDS can utilize AWS Lambda to provide more scalability and parallelism than would be practical than using just containers running in Docker or Kubernetes (AWS Lambda supports up to 1000-way parallelism by default).  When configured, HSDS will use Lambda for read operations that are likely to execute faster with Lambda than utilziing the existing set of conatiners.

Each Lambda invocation will be charged based on how long the code took to execute (typically less than 2 seconds) and memory used 
(configured below to 128MB).  These charges are minimal and would generally be much less than running an equivalent number of HSDS containers to provide a lambda level of capacity (though of course you should test for loads typical to your deployment).

To use Lambda, following the following steps in the Management Console:

1. In the AWS Management Console, select the Lambda service (for the same region in which you running HSDS)
2. Click the "Create Function" button
3. In the next page enter the name for the function as: "chunk_read"
4. Select "Python 3.8" in the Runtime dropdown menu
5. For "Execution Role", select (or create) a role that enables "AmazonS3ReadOnlyAccess" andd "AWSLambdaBasicExecutionRole"
6. Create the follwoing keys in the "Environment Variables" section:
    * `AWS_S3_GATEWAY - http://s3.amazon.com` (use endpoint for your region)
    * `LOG_LEVEL - INFO` (or ERROR or DEBUG if desired)
7. Leave the default values for Memory (128MB) and Timeout (30 seconds)

Next, build and deploy the lambda function:

1. In the HSDS source tree, cd to awslambda
2. Run: `./build.sh`.  This will create the lambda zip file
3. Run: `./deploy.sh`.  This will upload the zip to AWS
4. Refresh the management console, you should see the handler specified in the "Function Code" section

Next, configure HSDS to use lambda:

1. Setup environment variables as in "Sample .bashrc" below
2. Stop (`./stopall.sh`) and restart the service: (`./runall.sh`)

Testing
-------

When the server runs requests for hyperslab, point, or query functions that read data from a large number of chunks, 
the lambda function should be invoked.  Grepping for "serverless" in the SN log files will show when Lambda is invoked.

In the AWS Management console, go to the "Monitoring" tab to view metrics for Lambda invocations.  Lambda log files can also be
accessed from this page.


Sample .bashrc
--------------
These environment variables will be used by HSDS for AWS Lambda

    export AWS_LAMBDA_GATEWAY=https://lambda.us-east-1.amazonaws.com  # use Lambda endpoint for your region.
    # See: https://docs.aws.amazon.com/general/latest/gr/rande.html for list of endpoints
    export AWS_LAMBDA_CHUNKREAD_FUNCTION=chunk_read
 

