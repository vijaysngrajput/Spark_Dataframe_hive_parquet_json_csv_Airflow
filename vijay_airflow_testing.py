from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

dag = DAG('vijay_hello_world', description='Simple tutorial DAG',
          schedule_interval='@once',
          start_date=datetime(2019, 4, 17), catchup=False)

op1 = BashOperator(task_id='parquetToHive', bash_command='/usr/local/spark/bin/spark-submit /home/mapr/vijayPractise/parquetToHive.py', dag=dag)

op2 = BashOperator(task_id='hiveToCsv', bash_command='/usr/local/spark/bin/spark-submit /home/mapr/vijayPractise/hiveToCsv.py', dag=dag)

op3 = BashOperator(task_id='readFromHive', bash_command='/usr/local/spark/bin/spark-submit /home/mapr/vijayPractise/readFromHive.py', dag=dag)

op1 >> op2 >> op3

