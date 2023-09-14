# Project Title

[Short description of the project]

## Installation and Deployment

To complete the deployment of the files in this directory, run the Python files in the following order:

1. `A2I_cognito_side_creation.py`
    - This script handles the creation of Cognito resources required for the project.
  
2. `init_tables_data_functions.py`
    - This script initializes the tables and populates them with initial data.

3. `A2I_workflows_creation.py`
    - This script sets up the necessary A2I workflows.

4. `A2I_adding_users_to_cognito_pool.py`
    - This script adds users to the created Cognito pool.

Run the files from the command line using:

```bash
python A2I_cognito_side_creation.py
python init_tables_data_functions.py
python A2I_workflows_creation.py
python A2I_adding_users_to_cognito_pool.py
