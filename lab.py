from __future__ import print_function
import airflow
import pytz
import logging
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

def verifyWgetReturnCode(*args, **kwargs):

    #get country code from kwargs
    country = kwargs['country']
    logging.info('Verifying Curl return code for country: %s', country)

    #create task instance from kwargs
    task_instance = kwargs['ti']

    #pull wget return code from XCom
    upstream_task_id = 'download_file_{}'.format(
      country
    )
    curl_code = task_instance.xcom_pull(
      upstream_task_id
    )
    logging.info('CURL return code from task %s is %s',
      upstream_task_id,
      curl_code
    )

    # Return True if return code was 0 (no errors)
    if(curl_code == '0'):
        return True
    else:
        return False

def transformData(country, ds_nodash, *args, **kwargs):
  logging.info('Transforming data file')
  logging.info('Country: %s', country)
  logging.info('Date: %s', ds_nodash)

  # read file into a dataframe
  df = pd.read_csv('/tmp/data_{}_{}.csv'.format(
    country,
    ds_nodash
  ), sep=',',  error_bad_lines=False, skiprows=range(0, 1))

  logging.info('Dimensions: %s', df.shape)

  #rename columns, according to the schema
  df.columns = ['name', 'address', 'company', 'salary']

  #add `load_datetime` column with current timestamp
  df['load_datetime'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

  #change `partner_report_date` column to YYYY-MM-DD format

  #write to a combined file
  with open('/tmp/data_combined_{}.csv'.format(ds_nodash), 'a') as f:
    df.to_csv(f, header=False, sep=',')



start_date = datetime(2018, 06, 17, 0, 0, 0, tzinfo=pytz.utc)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'schedule_interval': None,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)

}

dag = DAG('lab3',
          description = 'Using PythonOperator for file processing',
          schedule_interval = None,
          default_args = default_args)


my_country_list = Variable.get('my_country_list').split(',')

upload_to_gcs = FileToGoogleCloudStorageOperator(
    task_id = 'upload_to_gcs',
    dst = 'airflow-testing/data/data_combined_{{ ds_nodash }}.csv',
    bucket = '{{ var.value.project_bucket }}',
    conn_id = 'google_cloud_default',
    trigger_rule='all_done', 
    src = '/tmp/data_combined_{}.csv'.format(
      '{{ ds_nodash }}'
    ),
    dag = dag
)

ingest_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id = 'ingest_to_bq',
    bucket='{{var.value.project_bucket}}',
    source_objects = ['airflow-testing/data/data_combined_{{ ds_nodash }}.csv'],
    destination_project_dataset_table = '{{ var.value.data_bq_dataset }}.user_profile_{{ ds_nodash }}',
    schema_fields = [  {'name': 'name', 'type': 'string', 'mode': 'nullable'},
      {'name': 'address', 'type': 'string', 'mode': 'nullable'},
      {'name': 'company', 'type': 'string', 'mode': 'nullable'},
      {'name': 'salary', 'type': 'integer', 'mode': 'nullable'} ],
    source_format = 'CSV',
    field_delimiter = ',',
    bigquery_conn_id = 'bigquery_default',
    google_cloud_storage_conn_id='google_cloud_default',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition = 'WRITE_TRUNCATE',
    dag = dag
)

for country in my_country_list:
  download_file = BashOperator(
    task_id = 'download_file_{}'.format(
      country
    ),
    bash_command = 'curl -o /tmp/data_{}_$EXEC_DATE.csv $URL/{}/data_{}.csv; echo $?'.format(
      country, 
      country,
      country
    ),
    env={'URL': '{{ var.value.test_file_url }}',
      'EXEC_DATE': '{{ ds_nodash }}'},
    xcom_push = True, 
    dag = dag
  ) 

  verify_download = ShortCircuitOperator(
    task_id = 'verify_download_{}'.format(
      country
    ),
    python_callable = verifyWgetReturnCode,
    provide_context = True,
    op_kwargs = [('country', country)],
    dag=dag
  )

  transform = PythonOperator(
    task_id = 'transform_data_{}'.format(
      country
    ),
    python_callable = transformData,
    provide_context = True,
    op_kwargs = [('country', country)],
    dag=dag
  )

  


  download_file >> verify_download >> transform >> upload_to_gcs

upload_to_gcs >> ingest_to_bq
                 