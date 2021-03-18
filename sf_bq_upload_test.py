# Connection un airflow: gcp_conn_adh

from google.cloud import bigquery
from google.oauth2 import service_account
import json

def upload_to_bq(clientBq, table_id, gs_uri):
    """
    clientBq: BigQuery connection\n
    table_id: BigQuery Table name\n
    gs_uri: url of the gs bucket including the file name
    """
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,
                                        autodetect=True,)
    load_job = clientBq.load_table_from_uri(uri, table_id, job_config=job_config)
    
    load_job.result()  # Waits for the job to complete.
    destination_table = clientBq.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

if __name__ == "__main__":
    
    service_account_info = json.load(open('C:/Users/andalarcon/OneDrive - ENDAVA/Documents/01_proyectos/2021/VideoAmp/ADH/keys/keyfile.json')) #test para sacar el json del keyfile

    credentials = service_account.Credentials.from_service_account_info(service_account_info)

    clientBq = bigquery.Client(credentials=credentials, project=credentials.project_id,)

    upload_to_bq(clientBq, "entity-reader-service.adh_upl_test.test4", "gs://andres_test/data_0_0_0.snappy.parquet") #el nombre del archivo en gs actualmente esta colocando el sufijo _0_0_0 cuando se realiza el copy into desde snowflake