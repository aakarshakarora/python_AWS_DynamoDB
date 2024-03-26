from datetime import datetime

import boto3

# Initialize DynamoDB client
dynamodb = boto3.client('dynamodb')

# Name of the DynamoDB table
VISIT_COUNTER_TABLE_NAME = 'VisitCounter'


def lambda_handler(event, context):
    # Increment visit counter
    increment_visit_counter()

    return {
        'statusCode': 200,
        'body': 'Visit counter incremented successfully!'
    }


def increment_visit_counter():
    try:
        # Try to get the current counter value
        response = dynamodb.get_item(
            TableName=VISIT_COUNTER_TABLE_NAME,
            Key={'CounterName': {'S': 'TotalVisits'}}
        )
        if 'Item' in response:
            # Counter exists, increment it
            counter = int(response['Item']['Count']['N'])
            counter += 1
            dynamodb.put_item(
                TableName=VISIT_COUNTER_TABLE_NAME,
                Item={'CounterName': {'S': 'TotalVisits'}, 'Count': {'N': str(counter)}}
            )
        else:
            # Counter doesn't exist, initialize it with 1
            dynamodb.put_item(
                TableName=VISIT_COUNTER_TABLE_NAME,
                Item={'CounterName': {'S': 'TotalVisits'}, 'Count': {'N': '1'}}
            )
    except Exception as e:
        print(f"Error incrementing visit counter: {e}")


def publish_to_sns(message):
    sns_client = boto3.client('sns')
    print("jjdd")
    sns_client.publish(
        TopicArn='arn:aws:sns:ap-south-1:058264108010:SNS2DB',
        Message=message
    )


def insert_into_instagram_post(item_data):
    # Insert item into Instagram_Post DynamoDB table
    dynamodb = boto3.resource('dynamodb')
    instagram_post_table = dynamodb.Table('Instagram_Post')

    response = instagram_post_table.put_item(Item=item_data)
    print(f'Insert response: {response}')

    # Publish to SNS topic
    print("Instagram_Post")
    publish_to_sns("New post inserted into Instagram_Post table")


# Sample item data to insert into Instagram_Post table
sample_item_data = {
    'user_name': 'dummy-12',
    'email_id': 'dummy12@email.com',
    'status': 'active',
    'created_date': datetime.now().isoformat()
}

if __name__ == "__main__":
    # Insert sample item into Instagram_Post table
    insert_into_instagram_post(sample_item_data)
