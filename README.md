# Summary
The purpose of this service is to read sierra bib/item ids from the SierraBibRequest/SierraItemRequest stream, retrieves those records from the sierra-wrapper (https://www.npmjs.com/package/sierra-wrapper) and publishes them to the SierraBibPostRequest/SierraItemPostRequest Stream

# Setup
1. Clone the repo to your local machine
2. Assuming you have node installed properly in your machine, run npm install
2. Run node-lambda setup, this should create a context.json, deploy.env and env files
3. Create a local.json file under config and add the following keys to it:
    {
        "key": "your_sierra_api_key",
        "secret": "your_sierra_secret",
        "isABib": true or false
    }
    local.json file should not be checked in and should be part of gitignore.
4. In your .env file provide values as needed, here is a sample - 

    AWS_ENVIRONMENT=development
    AWS_ACCESS_KEY_ID=APELOMDIH07AM7VROR4X
    AWS_SECRET_ACCESS_KEY=Mkp4JNpV10Ul+0z516hGnEPpiOC3xPvL4bd90tSZX
    AWS_PROFILE=
    AWS_SESSION_TOKEN=
    AWS_ROLE_ARN=arn:aws:iam::214409875932:role/lambda_basic_execution
    AWS_REGION=us-east-1
    AWS_FUNCTION_NAME=lambda_function_name
    AWS_HANDLER=index.handler
    AWS_MEMORY_SIZE=128
    AWS_TIMEOUT=3
    AWS_DESCRIPTION=
    AWS_RUNTIME=nodejs4.3
    AWS_VPC_SUBNETS=
    AWS_VPC_SECURITY_GROUPS=
    EXCLUDE_GLOBS="event.json"
    PACKAGE_DIRECTORY=build    

5. To run locally, the following event.json file is also added as part of the repo.
Now run the following command, node-lambda run

# Deployment
If the .env file is filled with the right values, then 
run, node-lambda deploy. Then add kinesis stream trigger to the lambda function


