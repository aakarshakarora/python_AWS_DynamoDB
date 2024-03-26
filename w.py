import boto3

# Initialize DynamoDB client
dynamodb = boto3.client('dynamodb')

TABLE_NAME = 'VisitCounter'


def lambda_handler(event, context):
    # Increment invocation counter
    print(event)
    increment_counter(context.function_name)

    return {
        'statusCode': 200,
        'body': 'Lambda function executed successfully!'
    }


def increment_counter(function_name):
    try:
        # Try to get the current counter value
        response = dynamodb.get_item(
            TableName=TABLE_NAME,
            Key={'FunctionName': {'S': function_name}}
        )
        if 'Item' in response:
            # Counter exists, increment it
            counter = int(response['Item'].get('InvocationCount', {'N': '0'})['N'])
            counter += 1
            dynamodb.put_item(
                TableName=TABLE_NAME,
                Item={'FunctionName': {'S': function_name}, 'InvocationCount': {'N': str(counter)}}
            )
        else:
            # Counter doesn't exist, initialize it with 1
            dynamodb.put_item(
                TableName=TABLE_NAME,
                Item={'FunctionName': {'S': function_name}, 'InvocationCount': {'N': '1'}}
            )
    except Exception as e:
        print(f"Error incrementing counter: {e}")


if __name__ == '__main__':
    # Sample event object
    sample_event = {
        "key1": "value1",
        "key2": "value2"
    }

    # Sample context object
    class SampleContext:
        function_name = "YourFunctionName"
        aws_request_id = "YourRequestID"
        invoked_function_arn = "YourFunctionARN"
        memory_limit_in_mb = "128"
        log_group_name = "YourLogGroupName"
        log_stream_name = "YourLogStreamName"

    # Call lambda_handler with sample event and context
    lambda_handler(sample_event, SampleContext())
