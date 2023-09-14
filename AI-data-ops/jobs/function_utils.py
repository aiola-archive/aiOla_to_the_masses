import datetime
import json
import logging
import boto3
from boto3.dynamodb.conditions import Key


def this_time_24_hours_ago():
    # Query ex.: WHERE "execution_start_timestamp" > TIMESTAMP '2023-07-09 10:07:05.000';
    return (datetime.datetime.now() - datetime.timedelta(weeks=12)).strftime("%Y-%m-%d %H:%m:%S.000")

def query_flow_definition_arn_by_flow_name(workflow_name):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('a2i_workflows_service_table')
    key_condition_expression = Key('a2i_workflow_name').eq(workflow_name)
    response = table.query(KeyConditionExpression=key_condition_expression)
    items_lst = []
    for item in response['Items']:
        items_lst.append(item['workflow_arn'])

    logging.info(f'{workflow_name} was set with the following arn: {items_lst[0]}')
    return items_lst[0]

def handle_ddb_param(ddb_param_value):
    ddb_param_value = str(ddb_param_value) # ensure ddb_param_value is always string
    
    if ddb_param_value.replace('.','').isdigit() and '.' in ddb_param_value: # --> Decimal
        return ddb_param_value
    elif ddb_param_value.isdigit(): # --> Integer
        return ddb_param_value
    elif ddb_param_value.isalpha():
        return ddb_param_value
    else:
        logging.info("ddb_param_value was not changed due to unfamiliar type of value. return as entered")
        return ddb_param_value
    
def concat_s3_key_for_a2i(workflow_name, logic_name, param_name):
    return f"/a2i/{workflow_name}-{logic_name}-{param_name}"

def service_and_table_details(workflow_instance):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('a2i_workflows_service_table')
    items = table.scan()['Items']
    assosiated_table_service = [item for item in items if item['a2i_workflow_name'] == workflow_instance.workflow_name][0].get('assosiated_table_service')
    assosiated_table_name = [item for item in items if item['a2i_workflow_name'] == workflow_instance.workflow_name][0].get('assosiated_table_name')
    return assosiated_table_service, assosiated_table_name

def check_file_exists(bucket, key):
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as e:
        return False
    
def write_json_to_s3(data, bucket_name, s3_key, s3_file_name):
    s3 = boto3.resource('s3')

    # Convert the Python object to a json string.
    json_data = json.dumps(data)

    # Get the current date.
    current_date = datetime.datetime.now()
    year, month, day = current_date.year, current_date.month, current_date.day

    # Construct the full file path including the key and date partition.
    full_file_path = f"{s3_key}/year={year}/month={month}/day={day}/{s3_file_name}"

    # Write the json string to a file in S3.
    s3.Object(bucket_name, full_file_path).put(Body=json_data)