from datetime import datetime

from boto3 import resource
from boto3.dynamodb.conditions import Attr, Key

demo_table = resource('dynamodb').Table('demo-dynamo-python')
# official documentation: https://docs.aws.amazon.com/code-library/latest/ug/python_3_dynamodb_code_examples.html

#############################  insert record #############################
"""
def insert():
    print('demo_insert')
    response = demo_table.put_item(
        Item={
            'customer_id': 'cus-03',  # parition key
            'order_id': 'ord-1',  # sort key
            'status': 'processed',
            'created_date': datetime.now().isoformat()
        }
    )
    print(f'Insert response: {response}')


insert()
"""


############################# Scan by attributes  #############################

def select_scan():
    print('demo_select_scan')
    filter_expression = Attr('status').eq('pending')

    item_list = []
    dynamo_response = {'LastEvaluatedKey': False}

    """
    In AWS, specifically in the context of Amazon DynamoDB, LastEvaluatedKey is a property returned by a query or scan operation when there are more results available. 
    DynamoDB paginates the results of query and scan operations if the result set size exceeds 1 MB of data. 
    When this happens, the response will include LastEvaluatedKey, which is the primary key of the last item that was evaluated. 
    This key can then be used in a subsequent query or scan operation to retrieve the next set of results.   
    """
    while 'LastEvaluatedKey' in dynamo_response:
        if dynamo_response['LastEvaluatedKey']:
            dynamo_response = demo_table.scan(  # read all entry and it is very costly
                FilterExpression=filter_expression,
                ExclusiveStartKey=dynamo_response['LastEvaluatedKey']
            )
            print(f'response-if: {dynamo_response}')
        else:
            dynamo_response = demo_table.scan(
                FilterExpression=filter_expression,
            )
            print(f'response-else: {dynamo_response}')

        for i in dynamo_response['Items']:
            item_list.append(i)

    print(f'Number of input tasks to process: {len(item_list)}')
    for item in item_list:
        print(f'Item: {item}')


# select_scan()

#############################  Query by parition key  #############################

def query_by_partition_key(customer_value):
    print('demo_select_query')

    response = {}
    filtering_exp = Key('customer_id').eq(customer_value)
    response = demo_table.query(
        KeyConditionExpression=filtering_exp)
    item_list = response["Items"]
    for item in item_list:
        print(f'Item: {item}')


# query_by_partition_key('cus-01')


#############################  Query by parition key and sort by ASC,DESC #############################

def query_by_partition_key_order(customer_value):
    print('\n\t\t\t>>>>>>>>>>>>>>>>> demo_query_by_partition_key_order <<<<<<<<<<<<<<<<<<<<<<')
    response = {}
    filtering_exp = Key('customer_id').eq(customer_value)
    response = demo_table.query(
        KeyConditionExpression=filtering_exp,
        ScanIndexForward=False)

    item_list = response["Items"]
    for item in item_list:
        print(f'Item: {item}')


# query_by_partition_key_order('cus-01')

#############################  Query by Global index and Local index #############################

def query_by_index_key(status_value):
    print('\n\t\t\t>>>>>>>>>>>>>>>>> demo_query_index_key <<<<<<<<<<<<<<<<<<<<<<')

    filtering_exp = Key('status').eq(status_value)
    response = demo_table.query(
        IndexName="status-index",
        KeyConditionExpression=filtering_exp,
        ScanIndexForward=False)

    for item in response["Items"]:
        print(f'Item: {item}')


# query_by_index_key('pending')

#############################  Query by parition and sort key #############################

def query_by_partition_key_and_sort_key(customer_value, order_value):
    print('\n\t\t\t>>>>>>>>>>>>>>>>> demo_query_by_partition_key_and_sort_key <<<<<<<<<<<<<<<<<<<<<<')

    response = {}
    filtering_exp = Key('customer_id').eq(customer_value)
    filtering_exp2 = Key('order_id').eq(order_value)
    response = demo_table.query(
        KeyConditionExpression=filtering_exp & filtering_exp2)
    # python literal and is not allowed

    for item in response["Items"]:
        print(f'Item: {item}')


# query_by_partition_key_and_sort_key('cus-01', 'ord-1')


#############################  Update record - start #############################

def update(customer_value, status_value):
    print('\n\t\t\t>>>>>>>>>>>>>>>>> demo_update <<<<<<<<<<<<<<<<<<<<<<')
    response = demo_table.update_item(
        Key={
            'customer_id': customer_value,
        },
        UpdateExpression='set status=:r, updated_date=:d',
        ExpressionAttributeValues={
            ':r': status_value,
            ':d': datetime.now().isoformat()
        },
        ReturnValues="UPDATED_NEW"
    )
    print(response, "Update")


def update_with_expression_name(customer_value, status_value):
    print('\n\t\t\t>>>>>>>>>>>>>>>>> demo_update_with_expression_name <<<<<<<<<<<<<<<<<<<<<<')
    response = demo_table.update_item(
        Key={
            'customer_id': customer_value,
            'order_id': 'ord-3'
        },
        UpdateExpression='set #status=:r, updated_date=:d',
        ExpressionAttributeValues={
            ':r': status_value,
            ':d': datetime.now().isoformat()
        },
        ExpressionAttributeNames={
            '#status': 'status'
        },
        ReturnValues="UPDATED_NEW"
    )
    print(f'Response: {response}')


# update_with_expression_name('cus-02', 'completed')

#############################  Update record - end #############################


#############################  Batch delete #############################
def batch_delete_transaction_records(items_to_delete):
    print('Deleting transactions')
    with demo_table.batch_writer() as batch:
        for item in items_to_delete:
            response = batch.delete_item(Key={
                "customer_id": item["id"],  # Change key and value names
                "order_id": item["order_id"]
            })
            print(response)


items = [{"id": "cus-04", "order_id": "ord-4"}, {"id": "cus-05", "order_id": "ord-4"}]
batch_delete_transaction_records(items)


"""
Reference:
https://chat.openai.com/share/9953f863-b548-44c8-b552-89fbe5d7e623

https://www.youtube.com/watch?v=x8IxY4zoBGI

https://docs.aws.amazon.com/code-library/latest/ug/python_3_dynamodb_code_examples.html

https://www.youtube.com/watch?v=Rp-A84oh4G8
"""
