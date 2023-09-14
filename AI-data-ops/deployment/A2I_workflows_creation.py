import boto3
import json
import uuid
import random
import awswrangler as wr
import logging

# Configure logging
logging.getLogger().setLevel(logging.INFO)

# Initialize AWS Services
sagemaker = boto3.client('sagemaker')
a2i = boto3.client('sagemaker-a2i-runtime')

def create_human_review_task_ui(task_ui_name, filename):
    with open(filename, 'r') as file:
        template = file.read()

    response = sagemaker.create_human_task_ui(
        HumanTaskUiName=task_ui_name,
        UiTemplate={'Content': template}
    )
    logging.info(response)

def create_or_skip_workflows(task_ui_name, role_arn, S3_BUCKET):
    for prefix in ['group_name1', 'group_name2', 'group_name3']:
        S3_bucket_prefix = 'your_prefix'

        flow_definitions = sagemaker.list_flow_definitions(MaxResults=50)['FlowDefinitionSummaries']
        if 'your_workflow_name' not in [flow_def['FlowDefinitionName'] for flow_def in flow_definitions]:

            S3Uri = f's3://{S3_BUCKET}/{S3_bucket_prefix}/inputs/'
            S3OutputPath = f's3://{S3_BUCKET}/{S3_bucket_prefix}/outputs/'

            human_task_uis = sagemaker.list_human_task_uis(MaxResults=50)['HumanTaskUiSummaries']
            workteams = sagemaker.list_workteams(MaxResults=50)['Workteams']

            human_task_ui_arn = [ht for ht in human_task_uis if ht['HumanTaskUiName'] == task_ui_name][0]['HumanTaskUiArn']
            workteam_arn = [wt['WorkteamArn'] for wt in workteams if wt['WorkteamName'] == f"{prefix.replace('_', '-')}-workteam"][0]

            response = sagemaker.create_flow_definition(
                FlowDefinitionName=f'your-workflow-name',
                HumanLoopConfig={
                    'WorkteamArn': workteam_arn,
                    'HumanTaskUiArn': human_task_ui_arn,
                    'TaskTitle': 'Your tasks general title',
                    'TaskDescription': 'Your tasks general description',
                    'TaskCount': 1,
                    'TaskAvailabilityLifetimeInSeconds': 600,
                    'TaskTimeLimitInSeconds': 3600,
                    'TaskKeywords': ['your keyword'],
                },
                OutputConfig={
                    'S3OutputPath': S3OutputPath
                },
                RoleArn=role_arn,
            )
            logging.info(f"Flow_definition: {response['FlowDefinitionArn']}")
        else:
            logging.info("Flow_definition already exists. Skipping.")

def main():
    # Constants and Environment Variables
    ENV = 'YOUR AWS ENV NAME'
    ACCOUNT = 'YOUR AWS ACCOUNT ID'
    REGION = 'YOUR PROJECT REGION'
    S3_BUCKET = 'YOUR DEDICATED S3 BUCKET NAME'
    your_role_arn = 'your_dedicated_iam_role_arn'

    # Initialize UI Creation
    task_ui_name = 'ai_model_template_name'
    filename = f"templates/{task_ui_name}.html"
    create_human_review_task_ui(task_ui_name, filename)

    # Create or Skip Workflows
    create_or_skip_workflows(task_ui_name, your_role_arn, S3_BUCKET)

if __name__ == "__main__":
    main()
