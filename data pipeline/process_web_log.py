#library import
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import datetime as dt 

#DAG arguments
default_args = {
    'owner':'me',
    'start_date':dt.datetime(2022, 10, 1),
    'email':'someone@gmail.com',
    'retries':1,
    'retry_delay': dt.timedelta(minutes=5),
}

dag = DAG(
    'process_web_log',
    description='access to web log',
    default_args=default_args,
    schedule_interval=dt.timedelta(minutes=30),
)

#extract ipaddress field from web server log file
task1 = BashOperator(
    task_id='Extract_data',
    bash_command='cut -d " " -f 1 /home/project/airflow/dags/capstone/accesslog.txt > /home/project/airflow/dags/capstone/extracted_data.txt',
    dag=dag,
)

# filter ipaddress "198.46.149.143" task
task2 = BashOperator(
    task_id='transform',
    bash_command='grep -e 198.46.149.143 < /home/project/airflow/dags/capstone/extracted_data.txt > /home/project/airflow/dags/capstone/transform_data.txt',
    dag=dag,
)

# load data task
task3 = BashOperator(
    task_id='load',
    bash_command='tar cvfz weblog.tar < /home/project/airflow/dags/capstone/transform_data.txt',
    dag=dag,
)

# task pipeline

task1 >> task2 >> task3