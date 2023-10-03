### Dependencies

At the moment, the only dependency you need is boto3.
You can install this into your dev environment using:
```commandline
pip install boto3
```

For now, I have deliberately avoided installing pydantic, even though pydantic will make the code look nicer.
The reason is that installing lambda dependencies can be fiddly. We should optimise for minimising the chances
of installation errors, since that will annoy potential users.


### Deploying the Lambda function

1. Create a new Lambda function with:
    - a Python 3.11 runtime
    - a public URL (so that our control loop can make API requests to it)
    - a default IAM policy (so it can write to Cloudwatch logs for debugging purposes)

2. Add an IAM policy to this Lambda function's IAM role, containing the permissions in iam_permissions.json.
(Of course, edit these permissions if the ECS cluster name or the ECS task/execution role names change.)

3. Add the environment variables in environment.env, changing them as necessary.

4. Set the Lambda timeout to something a little more generous, e.g. 10 seconds, just in case.

5. Copy the code from lambda_handler.py into the Lambda function.
