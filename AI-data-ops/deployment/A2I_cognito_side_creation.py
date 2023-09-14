import boto3
import json
import uuid
import random
import logging

logging.getLogger().setLevel(logging.INFO)

def main():
    # Initialize AWS SDK clients
    cognito_idp_client = boto3.client('cognito-idp')
    sagemaker_client = boto3.client('sagemaker')
    
    # Configuration settings
    pool_name = 'your_pool_name'
    resource_server = 'your_rs_name'
    user_pool_client = 'your_user_pool_client_name'
    app_client_name = 'your_app_client_name'
    
    # Create Cognito user pool
    user_pool_response = create_cognito_user_pool(cognito_idp_client, pool_name)
    
    # Add the pool with app integration
    add_pool_with_app_integration(cognito_idp_client, user_pool_response, resource_server, user_pool_client)

def create_cognito_user_pool(cognito_idp_client, pool_name):
    user_pools = cognito_idp_client.list_user_pools(MaxResults=60)['UserPools']
    if not any(pool['Name'] == pool_name for pool in user_pools):
        response = cognito_idp_client.create_user_pool(
            PoolName=pool_name,
            Policies={
                'PasswordPolicy': {
                    'MinimumLength': 8,
                    'RequireUppercase': True,
                    'RequireLowercase': True,
                    'RequireNumbers': True,
                    'RequireSymbols': True
                }
            },
            AutoVerifiedAttributes=['email'],
            AliasAttributes=['email'],
            AdminCreateUserConfig={
                'AllowAdminCreateUserOnly': True,
                'UnusedAccountValidityDays': 7,
                'InviteMessageTemplate': {
                    'EmailMessage': 'Your username is {username} and temporary password is {####}.',
                    'EmailSubject': 'Your temporary password'
                }
            },
            Schema=[
                {
                    'Name': 'email',
                    'AttributeDataType': 'String',
                    'DeveloperOnlyAttribute': False,
                    'Mutable': True,
                    'Required': True,
                    'StringAttributeConstraints': {
                        'MinLength': '0',
                        'MaxLength': '2048'
                    }
                }
            ]
        )
        logging.info(f"UserPool Id: {response['UserPool']['Id']}")
        return [user_pool for user_pool in cognito_idp_client.list_user_pools(MaxResults=60)['UserPools'] if user_pool['Name'] == pool_name][0]
    else:
        logging.info("User Pool already exists. Skipping.")
        return [user_pool for user_pool in cognito_idp_client.list_user_pools(MaxResults=60)['UserPools'] if user_pool['Name'] == pool_name][0]

def add_pool_with_app_integration(cognito_idp_client, user_pool_response, resource_server, user_pool_client):
    resource_servers = cognito_idp_client.list_resource_servers(UserPoolId=user_pool_response['Id'], MaxResults=50)['ResourceServers']
    if not any(server['Name'] == resource_server for server in resource_servers):
        resource_response = cognito_idp_client.create_resource_server(
            UserPoolId=user_pool_response['Id'],
            Identifier=resource_server,
            Name=resource_server
        )

        client_response = cognito_idp_client.create_user_pool_client(
            UserPoolId=user_pool_response['Id'],
            ClientName=user_pool_client,
            GenerateSecret=True,
            RefreshTokenValidity=30,
            AllowedOAuthFlows=['client_credentials'],
            AllowedOAuthFlowsUserPoolClient=True
        )

        logging.info(f"App Client ID: {client_response['UserPoolClient']['ClientId']}")
        logging.info(f"App Client Secret: {client_response['UserPoolClient']['ClientSecret']}")
    else:
        logging.info("App client ID and App client secret already exist. Skipping.")

def create_resource_server_and_client(cognito_idp_client, user_pool_response, resource_server, user_pool_client):
    if not any([True for server_res in cognito_idp_client.list_resource_servers(UserPoolId=user_pool_response['Id'], MaxResults=50)['ResourceServers'] if server_res['Name'] == resource_server]):
        resource_response = cognito_idp_client.create_resource_server(
            UserPoolId=user_pool_response['Id'],
            Identifier=resource_server,
            Name=resource_server
        )
        client_response = cognito_idp_client.create_user_pool_client(
            UserPoolId=user_pool_response['Id'],
            ClientName=user_pool_client,
            GenerateSecret=True,
            RefreshTokenValidity=30,
            AllowedOAuthFlows=['client_credentials'],
            AllowedOAuthFlowsUserPoolClient=True,
        )
        logging.info(f"App Client ID: {client_response['UserPoolClient']['ClientId']}")
        logging.info(f"App Client Secret: {client_response['UserPoolClient']['ClientSecret']}")
    else:
        logging.info("App client ID and App client secret already exist. Skipping.")

def create_app_client(cognito_idp_client, user_pool_response, app_client_name):
    if not any([
        True if obj.get('ClientName') == app_client_name else False
        for obj in cognito_idp_client.list_user_pool_clients(UserPoolId=user_pool_response['Id'], MaxResults=50).get('UserPoolClients')
    ]):
        client_id_response = cognito_idp_client.create_user_pool_client(
            UserPoolId=user_pool_response['Id'],
            ClientName=app_client_name,
            GenerateSecret=True,
        )
        logging.info(f"App Client ID: {client_id_response['UserPoolClient']['ClientId']}")
    else:
        logging.info("App client ID already exists. Skipping.")

def main():
    cognito_idp_client = boto3.client('cognito-idp')
    sagemaker_client = boto3.client('sagemaker')

    pool_name = 'your_pool_name'
    resource_server = 'your_rs_name'
    user_pool_client = 'your_user_pool_client_name'
    app_client_name = 'your_app_client_name'

    user_pool_response = create_cognito_user_pool(cognito_idp_client, pool_name)
    create_resource_server_and_client(cognito_idp_client, user_pool_response, resource_server, user_pool_client)
    create_app_client(cognito_idp_client, user_pool_response, app_client_name)

if __name__ == '__main__':
    main()
