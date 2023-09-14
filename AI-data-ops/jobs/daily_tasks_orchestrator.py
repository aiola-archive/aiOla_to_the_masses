import boto3
import os
import uuid
import datetime
import json
import logging
import awswrangler as wr
import pandas as pd
from pandasql import sqldf
from datetime import timedelta
from boto3.dynamodb.conditions import Attr

from function_utils import this_time_24_hours_ago, handle_ddb_param, concat_s3_key_for_a2i, service_and_table_details, check_file_exists, write_json_to_s3, prodigy_to_custom_format
from services import A2IWorkflow, AthenaQueryService, DDBQueryService, DDBParameterService, DDBLogicByTeamRepositoryService, A2ITaskCreationService, A2IEventData

logging.basicConfig(level=logging.INFO)

REGION = 'YOUR REGION'
ACCOUNT = 'YOUR ACCOUNT'
CLIENT = 'YOUR ENV'
S3_BUCKET = f'YOUR S3 BUCKET'

DATABASE = 'Your Athena Database'
TABLE = 'Your Athena Table'
S3_OUTPUT = f's3://{S3_BUCKET}/a2i/athena_results/'

def main():

    athena_instance = AthenaQueryService(database='ai_monitoring',
                                         table='tomer_aiola_inspection_ai_monitoring_v',
                                         s3_bucket=S3_BUCKET,
                                         output_key='/a2i/athena_results/')
    ddb_query_instance = DDBQueryService(table='a2i_sentence_generator_table')
    ddb_parameter_service_table = DDBParameterService(table_name='a2i_parameter_service_table')
    ddb_logic_conditions_service_table = DDBLogicByTeamRepositoryService(table_name='a2i_logic_conditions_service_table')

    # Matching each logic with each parameters
    # TODO: if works, delete "associated params from logics ddb"
    logics_df = pd.DataFrame(ddb_logic_conditions_service_table.get_all_logics())
    params_df = pd.DataFrame(ddb_parameter_service_table.get_all_parameters())

    for logic in logics_df.iterrows():
        logic = logic[1]

        # All parameters that points on that iterated logic
        logic_associated_params = []

        if logic.get('is_active') is True:
            logging.info(f"Iterating over '{logic.get('logic_name')}' {logic.get('logic_name')}.")

            # Creating workflow_instance
            workflow_instance = A2IWorkflow(workflow_name=logic.get('related_workflow'))

            for param in params_df.iterrows():
                param = param[1]

                if param.get('is_active'):
                    if param.get('associated_logic') == logic.get('logic_name'):

                        logic_associated_params.append(param.get('parameter_name'))

        for associated_param_name in logic_associated_params:
            param_item = ddb_parameter_service_table.get_parameter(associated_param_name)

            logging.info(f"Task assosiated with {param_item.get('team')} team.")

            query_where_condition = logic.get('query_where_condition').replace('parameter', handle_ddb_param(param_item.get('value')))
            
            assosiated_table_service, assosiated_table_name = service_and_table_details(workflow_instance)
            # Queries all events from the last 24 hours
            # For each event, a human taks will be opened
            if assosiated_table_service == 'athena': # Positioned here since every query required different source or table
                events_df = athena_instance.query_data()
            elif assosiated_table_service == 'dynamodb':
                ddb_query_instance.update_table(assosiated_table_name)
                
                # Calculate datetime 24 hours ago, in ISO-8601 format
                time_24_hours_ago = (datetime.datetime.now() - timedelta(days=1)).isoformat()

                # Create filter expression
                filter_expression = Attr('datetime').gte(time_24_hours_ago)
                events_df = ddb_query_instance.query_data(filter_expression)

                # Minor data formatting
                if events_df.shape[0] > 0:
                    for col in ['sentence_id', 'batch_id']:
                        events_df[col] = events_df[col].apply(lambda x: float(x))
            else:
                logging.info("Error, query for data did not happen. please check data fields in ddb table named 'a2i_workflows_service_table'.")
                break

            # Filtering results of Athena or DDB queries results
            if events_df.shape[0] > 0:
                pysqldf = lambda q: sqldf(q, globals())
                query = f"SELECT * FROM events_df WHERE {query_where_condition}"
                filtered_events_df = pysqldf(query)
                logging.info(f'found {filtered_events_df.shape[0]} events that met condition. opening tasks.')
            else:
                logging.info(f'found {events_df.shape[0]} events that met condition. opening tasks.')
                break
            
            logging.info(f'Comparing events vs. logic+params...')
            # If condition is met, enter condition
            for index, row in filtered_events_df.iterrows():
                # Creating event instance
                event_instance = A2IEventData(workflow_name=workflow_instance.workflow_name)
                event_instance.set_event_data(row.fillna(value="None").to_dict())

                new_event_data = event_instance.event_data

                if isinstance(new_event_data, str):
                    new_event_data = json.loads(new_event_data)

                if event_instance.event_data is None: # Is None
                    logging.info("'event_instance.event_data' is None. please check process")
                    break

                new_event_data['task_desc'] = json.dumps(
                    {
                        'task_uuid': str(uuid.uuid4()),
                        'human_loop_name': param_item.get('parameter_name'),
                        "s3_key": concat_s3_key_for_a2i(
                            workflow_name=workflow_instance.workflow_name,
                            logic_name=logic.get('logic_name'),
                            param_name=param_item.get('parameter_name')
                        )
                    }
                )
            
                if logic.get('related_workflow') == 'nlp-tagging-workflow' and new_event_data.get('nlp_prediction_prodigy') is not None:

                    new_event_data['nlp_prediction_prodigy_for_ui'] = prodigy_to_custom_format(new_event_data.get('nlp_prediction_prodigy'))

                event_instance.update_formatted_event_data(new_event_data)
                
                if not new_event_data.get('is_error'):
                    # Creating task instance
                    a2i_task_instance = A2ITaskCreationService(flow_definition_arn=workflow_instance.flow_definition_arn, 
                                        s3_bucket=S3_BUCKET,
                                        s3_key=concat_s3_key_for_a2i(workflow_instance.workflow_name, logic.get('logic_name'), param_item.get('parameter_name'))
                    )

                    # Setting task_instance data
                    a2i_task_instance.set_human_loop_input(event_instance.event_data)
                    a2i_task_instance.create_task()
                else:
                    logging.info('There was an error with the input data. saved the event output to the S3 for debugging. file name == execution_arn')
                    write_json_to_s3(new_event_data, S3_BUCKET, 'a2i/corrupt_events_ai_monitoring/', new_event_data.get('execution_arn'))
                    


if __name__ == "__main__":
        main()
