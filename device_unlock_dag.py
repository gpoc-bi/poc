import datetime
import os

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule

BUCKET = models.Variable.get('gcs_bucket')  # GCS bucket with our data.

PYSPARK_JOB1 = 'gs://device_unlock_inbound/Device_Unlock_OL_GCP_run.py'
PYSPARK_JOB2 = 'gs://device_unlock_inbound/device_unlock_el_cloud_run.py'

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': models.Variable.get('gcp_project')
}

with models.DAG(
        'Device_unlock_run_g',
        # Continue to run DAG once per day
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    # Create a Cloud Dataproc cluster.
    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
      task_id='create_dataproc_cluster',
      # Give the cluster a unique name by appending the date scheduled.
      # See https://airflow.apache.org/code.html#default-variables
      cluster_name='composer-hadoop-tutorial-cluster-{{ ds_nodash }}',
      num_workers=2,
      zone=models.Variable.get('gce_zone'),
      master_machine_type='n1-standard-2',
      worker_machine_type='n1-standard-2')
        

    # Submit the PySpark job.
    submit_pyspark1 = dataproc_operator.DataProcPySparkOperator(
        task_id='submit_ol_load_job',
        main=PYSPARK_JOB1,
        # Obviously needs to match the name of cluster created in the prior Operator.
        cluster_name='composer-hadoop-tutorial-cluster-{{ ds_nodash }}',
        dataproc_jars  = 'gs://spark-lib/bigquery/spark-bigquery-latest.jar',
        dataproc_pyspark_jars ='gs://spark-lib/bigquery/spark-bigquery-latest.jar')
        
        
    submit_pyspark2 = dataproc_operator.DataProcPySparkOperator(
         task_id='submit_el_transform_load_job',
         main=PYSPARK_JOB2,
         # Obviously needs to match the name of cluster created in the prior Operator.
         cluster_name='composer-hadoop-tutorial-cluster-{{ ds_nodash }}',
         dataproc_jars  = 'gs://spark-lib/bigquery/spark-bigquery-latest.jar',
         dataproc_pyspark_jars ='gs://spark-lib/bigquery/spark-bigquery-latest.jar')
        
create_dataproc_cluster >> submit_pyspark1 >> submit_pyspark2
		
#create_dataproc_cluster >> submit_pyspark1 >> submit_pyspark2