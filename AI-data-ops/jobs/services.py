# DEBUG!

import json
import boto3
import logging
from boto3.dynamodb.conditions import Key
import pandas as pd

import awswrangler as wr

from function_utils import *

class A2IWorkflow:
    def __init__(self, workflow_name) -> None:
        self.a2i_client = boto3.client('sagemaker-a2i-runtime')
        self.s3_client = boto3.client('s3')
        self.dynamodb = boto3.client('dynamodb')
        self.workflow_name = workflow_name
        self.flow_definition_arn = query_flow_definition_arn_by_flow_name(self.workflow_name)

class AthenaQueryService:
    def __init__(self, database, table, s3_bucket, output_key):
        self.database = database
        self.table = table
        self.s3_bucket = s3_bucket
        self.output_key = output_key
        self.s3_bucket_and_key = ('s3://' + self.s3_bucket + self.output_key)

    def query_data(self): # TODO: how to store the logic --> as a full query or potentially other way
        query = f""" 
                    SELECT *
                    FROM {self.database}.{self.table}
                    WHERE "execution_start_timestamp" > TIMESTAMP '{this_time_24_hours_ago()}'
                """
        print(query)
        return wr.athena.read_sql_query( # todo: potential --> migrate to something like "start query execution"
                    sql=query,
                    database=self.database,
                    s3_output=self.s3_bucket_and_key,
                    ctas_approach=False,
                )
    
class DDBQueryService:
    def __init__(self, table):
        self.dynamodb = boto3.resource('dynamodb')
        self.table = table

    def query_data(self, filter_expression): 
        from boto3.dynamodb.conditions import Key

        table = self.dynamodb.Table(self.table)
        # Condition is last 24 ours of 'datetime' attribute.
        # This means that each table with events, must have that attribute
        response = table.scan(
            FilterExpression=filter_expression
        )
        items = response['Items']
        return pd.DataFrame(items)
    
    def update_table(self, new_table):
        self.table = new_table

# Parameter service table structure #
# parameter_name | value_threshold | last_updatedate | description [what is it used from] | associated logic | team | value 

class DDBParameterService:
    def __init__(self, table_name):
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(table_name)

    def get_all_parameters(self):
        response = self.table.scan()
        return response['Items']

    def get_parameter(self, parameter_name):
        response = self.table.get_item(
            Key={
                'parameter_name': parameter_name
            }
        )
        return response['Item']


# Will be used as a "WHERE" clause repository, with dynamic parameters
# Logic by team repo service table structure #
# logic_name | team [NLP, ASR, SD_GENERAL] | query_where_condition [EX: len(jargon_transcript) > {param}] | last_updatedate | is_active | related_workflow | parameters [includes all parameters related to that logic]
class DDBLogicByTeamRepositoryService:
    def __init__(self, table_name):
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(table_name)

    def get_all_logics(self):
        response = self.table.scan()
        return response['Items']

    def get_logic_by_name(self, logic_name):
        response = self.table.get_item(Key={'parameter_name': logic_name})
        return response['Item']['logic']


class A2ITaskCreationService:
    def __init__(self, flow_definition_arn, s3_bucket, s3_key):
        self.a2i_runtime = boto3.client('sagemaker-a2i-runtime') 
        self.flow_definition_arn = flow_definition_arn
        self.human_loop_input = None # TODO: create function to structure this; ulimately, default input
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_bucket_and_key = ('s3://' + self.s3_bucket + self.s3_key)
        self.task_uuid = None

    def update_flow_definition(self, new_flow_definition_arn):
        self.flow_definition_arn = new_flow_definition_arn

    def update_s3_bucket_and_key(self):
        self.s3_bucket_and_key = ('s3://' + self.s3_bucket + self.s3_key)
        
    def update_s3_key(self, new_s3_key):
        self.s3_key = new_s3_key
        self.update_s3_bucket_and_key(self)

    def set_human_loop_input(self, human_loop_input):
        self.human_loop_input = human_loop_input
        self.task_uuid = json.loads(self.human_loop_input.get('task_desc')).get('task_uuid')

    # TODO: align task creator so format will be the same
    def create_task(self):

        if self.human_loop_input is not None and self.task_uuid is not None:
            response = self.a2i_runtime.start_human_loop(
                HumanLoopName=self.task_uuid,
                FlowDefinitionArn=self.flow_definition_arn,
                HumanLoopInput={
                    'InputContent': json.dumps(self.human_loop_input),
                },
                DataAttributes={
                    'ContentClassifiers': [
                        'FreeOfPersonallyIdentifiableInformation',
                        'FreeOfAdultContent',
                    ]
                }
            )
            logging.info(f'Human task created successfully.')
            return response
        
        else:
            logging.info('self.human_loop_input is None. Therefore, human task was not opened.')
            return None
        
def dummy_formatting_function(event_data):
    logging.info(f'Please alter the function to actually format your data in the wanted format')
    return event_data

def other_dummy_formatting_function(event_data):
    logging.info(f'Please alter the function to actually format your data in the wanted format')
    return event_data

class A2IEventData:

    def __init__(self, workflow_name):
        # self.client = boto3.client('lambda')
        self.dynamodb = boto3.resource('dynamodb')
        self.target_workflow = workflow_name
        self.raw_event_data = None
        self.event_data = None
        self.is_event_data_formatted = False

    # TODO: finish following fucntion --> lambdas creation, call, and return
    def format_event_data(self):

        # based on workflow_name, finding the associated lambda
        table = self.dynamodb.Table('a2i_workflows_service_table')
        response = table.query(
            KeyConditionExpression=Key('a2i_workflow_name').eq(self.target_workflow)
        )

        if response['Items'][0].get('associated_formatting_lambda') == 'a2i_nlp_ui_input_lambda':
            response_content = dummy_formatting_function(self.raw_event_data)

        elif response['Items'][0].get('associated_formatting_lambda') == 'a2i_asr_ui_input_lambda':
            response_content = other_dummy_formatting_function(self.raw_event_data)


        self.event_data = response_content.get('body')
        self.is_event_data_formatted = True
        logging.info(f'event_data was formatted. is_event_data_formatted was set to True')


    def set_event_data(self, event_data):
        self.raw_event_data = event_data
        self.format_event_data()

    def update_formatted_event_data(self, new_event_data):
        self.event_data = new_event_data

    def set_event_data_formatted_to_true(self):
        self.is_event_data_formatted = True

    def set_event_data_formatted_to_false(self):
        self.is_event_data_formatted = False

    def return_event_data(self):
        return self.event_data

    def show_event_data(self):
        print(f'raw_event_data: {self.raw_event_data}')
        print(f'event_data: {self.event_data}')
