
# coding: utf-8

# In[ ]:


from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators import S3TransferOperator
from airflow.operators import SnowflakeCopyOperator

dag = DAG('test_plugin', description='Test plugin call',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)


#s3_copy_operator = BashOperator(task_id='s3_copy', bash_command='python /home/ubuntu/airflow/scripts/s3transfer.py',dag=dag)
plugin_operator_task = S3TransferOperator(filepath='/home/ubuntu/',filename='title.ratings.tsv.gz', s3bucket='movie-etl-staging',path_to_key='/home/ubuntu/airflow/.credfiles',task_id='S3TransferOperatorTest',dag=dag)
snowflake_copy_operator = SnowflakeCopyOperator(filename='title.ratings.tsv.gz', s3bucket='movie-etl-staging',task_id='sf_copy', dag=dag)

plugin_operator_task >> snowflake_copy_operator

