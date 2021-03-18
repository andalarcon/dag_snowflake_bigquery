from airflow.hooks.base_hook import BaseHook
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.hooks.base_hook import BaseHook

import scripts.runner_functions as runner
import scripts.template_runner as template_runner
from scripts.snowflake_connector import JdbcSnowflakeConnector
import snowflake.connector

from google.cloud import bigquery
from google.oauth2 import service_account
import json

#Upload data to gs using the snowflake connector, its necvesary to specify the parameters (gs_bucket_filename, table, storage_integration), could be a table or another json
def create_update_gsfile(**kwargs):
    report_metadata = kwargs['dag_run'].conf.get('report_metadata')
    ms_hook = SnowflakeHook(snowflake_conn_id="snowflake_ms")
    conn_data = ms_hook.get_connection("snowflake_ms")
    data_dict = json.loads(conn_data.extra)
    data_dict['user'] = conn_data.login
    data_dict['password'] = conn_data.password
    jdbcsf = JdbcSnowflakeConnector(**data_dict)
    jdbcsf.connect()

    jdbcsf.run(query=f"""
    COPY INTO 'gcs://{gs_bucket_filename}'
        FROM {table}
        FILE_FORMAT = (type=parquet  null_if = ('NULL'))
        storage_integration = {storage_integration}
        OVERWRITE=TRUE
        HEADER=TRUE;
    """)
    jdbcsf.disconnect()

def upload_to_bq(**kwargs):
    
    # table_id: BigQuery Table name
    # gs_uri: url of the gs bucket including the file name
    
    connection = BaseHook.get_connection("gcp_conn_adh") #gcp_conn_adh is the connection with the json token for bigquery 
    json_token =  connection.json

    service_account_info = json_token

    credentials = service_account.Credentials.from_service_account_info(service_account_info)

    clientBq = bigquery.Client(credentials=credentials, project=credentials.project_id,)

    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,
                                        autodetect=True,)
    load_job = clientBq.load_table_from_uri(uri, table_id, job_config=job_config) #It's necesary to specify the variables uri and table_id
    
    load_job.result()  # Waits for the job to complete.
    destination_table = clientBq.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

dag = DAG('Sf_BigQuery_Upload',
          default_args=default_args,
          schedule_interval=None,
          max_active_runs=100,
        #   on_success_callback=on_success,
          catchup=False)

with dag:
    run_gsfile = PythonOperator(task_id="create_update_gsfile", provide_context=True, python_callable=create_update_gsfile)
    bq_upload = PythonOperator(task_id="upload_to_bq", provide_context=True, python_callable=upload_to_bq)

run_gsfile >> bq_upload