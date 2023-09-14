import boto3
import json
import datetime
import logging

logging.basicConfig(level = logging.INFO)

with open('python/env_config.json', 'r') as file:
    env_config = json.load(file)

REGION = env_config['REGION']
ACCOUNT = env_config['ACCOUNT']
CLIENT = env_config['CLIENT']
S3_BUCKET = f'{CLIENT}-aiola-{ACCOUNT}-inspection-data'

import boto3
import json
import datetime
import logging
from typing import List, Dict

logging.basicConfig(level=logging.INFO)

def create_ddb_tables(dynamodb, table_names: List[str], table_keys: List[str]) -> None:
    for table_name, table_key in zip(table_names, table_keys):
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': table_key,
                    'KeyType': 'HASH'
                },
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': table_key,
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        logging.info(f"Table {table_name} created with item count: {table.item_count}")


def load_data_to_table(dynamodb, table_name: str, data: List[Dict]) -> None:
    table = dynamodb.Table(table_name)
    for item in data:
        table.put_item(Item=item)
    logging.info(f"Data loaded to {table_name} successfully.")


def main():
    REGION = 'YOUR REGION'
    ACCOUNT = 'YOUR ACCOUNT'
    CLIENT = 'YOUR CLIENT'
    S3_BUCKET = f'{CLIENT}-aiola-{ACCOUNT}-inspection-data'

    dynamodb = boto3.resource('dynamodb')

    table_names = ['a2i_workflows_service_table', 'a2i_logic_conditions_service_table', 'a2i_parameter_service_table']
    table_keys = ['parameter_name', 'logic_name', 'parameter_name']

    create_ddb_tables(dynamodb, table_names, table_keys)

    # Sample data and tables to load the data into
    sample_data_for_parameter_table = [
        {
            "parameter_name": "parameter_name",
            "value": 'some_value_to_compare_with',
            "last_updatedate": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.000"),
            "description": "...",
            "team": "name_of_the_associated_team",
            "associated_logic": "name_of_the_associated_logic",
            "is_active": True
        }
    ]
    load_data_to_table(dynamodb, 'a2i_parameter_service_table', sample_data_for_parameter_table)

    sample_data_for_logic_table = [
        {
            'logic_name': 'logic_name',
            'query_where_condition': """some_SQL_where_clause_condition > parameter""",
            'last_updatedate': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.000"),
            'description': '...',
            'parameter_services': ['name_of_associated_parameter1', 'name_of_associated_parameter2'],
            'is_active': True,
            'related_workflow': 'name_of_the_a2i_workflow_associated_with_the_logic',
        }
    ]
    load_data_to_table(dynamodb, 'a2i_logic_conditions_service_table', sample_data_for_logic_table)

    sample_data_for_workflow_table = [
        {
            "a2i_workflow_name": "name_of_your_a2i_workflow",
            "workflow_arn": f"arn:aws:sagemaker:{REGION}:{ACCOUNT}:flow-definition/your_workflow_arn",
            "last_updatedate": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.000"),
            "assosiated_table_service": "name_your_workflow_data_source_service",
            "assosiated_table_name": "name_your_data_source_table_name"
        }
    ]
    load_data_to_table(dynamodb, 'a2i_workflows_service_table', sample_data_for_workflow_table)


if __name__ == '__main__':
    main()
