from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from linkedin import linkedin_etl
from airflow.utils.dates import days_ago


default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2023,11,4),
    'email':'neeravnilay@gmail.com',
    'email_on_failure':True,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(min=1)

}


dag = DAG(
    'linkedin_dag',
    default_args = default_args,
    schedule_interval=timedelta(days=1),
    description="Linkedin ETL Dag."
)   

run_etl = PythonOperator(
    task_id = 'linkedin-etl-program',
    python_callable = linkedin_etl,
    dag=dag

)

run_etl