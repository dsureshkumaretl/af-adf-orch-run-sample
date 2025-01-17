import os
from airflow_client.client import ApiClient, Configuration
from airflow_client.client.api import connection_api
from airflow_client.client.model.connection import Connection

# Fetch the connection URI from environment variables
connection_uri = os.getenv("AIRFLOW_CONN_AZURE_DATA_FACTORY_TEST")

# Configure the Airflow client
configuration = Configuration(
    host="https://fcdb0501971737.eastus.airflow.svc.datafactory.azure.com/api/v1",
    username="",  # Leave empty if no specific username
    password=""   # Leave empty if no specific password
)
api_client = ApiClient(configuration)

# Create an instance of the Connection API
conn_api = connection_api.ConnectionApi(api_client)

# Define the connection details
connection = Connection(
    conn_id="airflow_datafactory",
    conn_type="azure_data_factory",
    uri=connection_uri
)

# Create the connection
try:
    conn_api.post_connection(connection)
    print(f"Connection {connection.conn_id} created successfully.")
except Exception as e:
    print(f"Failed to create connection {connection.conn_id}: {e}")

# Fetch the connection details to verify
try:
    connection = conn_api.get_connection("airflow_datafactory")
    print(f"Connection details: {connection}")
except Exception as e:
    print(f"Failed to fetch connection: {e}")