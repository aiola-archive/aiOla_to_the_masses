import boto3

def create_cognito_user(pool_name, group_name, username, email, profile_value):
    """Create a new Cognito user and add them to a specified group."""

    # Initialize Cognito Identity Provider client
    cognito_idp_client = boto3.client('cognito-idp')

    # Retrieve the UserPool ID based on its name
    user_pools = cognito_idp_client.list_user_pools(MaxResults=60)['UserPools']
    user_pool_id = next(pool['Id'] for pool in user_pools if pool['Name'] == pool_name)

    # Create the user in the specified user pool
    response = cognito_idp_client.admin_create_user(
        UserPoolId=user_pool_id,
        Username=username,
        UserAttributes=[
            {'Name': 'email', 'Value': email},
            {'Name': 'profile', 'Value': profile_value}  # Used for future workteam association
        ],
        DesiredDeliveryMediums=['EMAIL'],
        ForceAliasCreation=True
    )

    print(f"User '{username}' created successfully.")

    # Add the user to the specified group in the user pool
    add_to_group_response = cognito_idp_client.admin_add_user_to_group(
        UserPoolId=user_pool_id,
        Username=username,
        GroupName=group_name
    )

    print(f"User '{username}' was added to the group '{group_name}' successfully.")


if __name__ == "__main__":
    # Sample variables (replace these with your actual values)
    pool_name = 'your_user_pool_name'
    group_name = 'your_group_name'
    usernames = ['sample_username1', 'sample_username2']
    emails = ['sample_email1@example.com', 'sample_email2@example.com']
    profile_values = ['profile_value1', 'profile_value2']

    # Iterate through usernames, emails, and profile values to create Cognito users
    for username, email, profile_value in zip(usernames, emails, profile_values):
        create_cognito_user(pool_name, group_name, username, email, profile_value)
