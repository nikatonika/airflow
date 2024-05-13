import pandas as pd
from datetime import datetime, date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from scripts.transform_script import transfrom

dag = DAG(
    "monthly_activity_dag_VeronikaZaslavskaia",
    schedule="0 0 5 * *",
    start_date=datetime(2024, 4, 30),
)

# Функция для загрузки данных
def extract(file_path: str, **context) -> None:
    df = pd.read_csv(file_path)
    context["ti"].xcom_push(key="extracted", value=df)


# Функция для преобразования данных
def transform_profit(**context) -> None:
    profit_table = context["ti"].xcom_pull(key="extracted", task_ids=["extract"])[0]
    df_tmp = transfrom(date=date.today(), profit_table=profit_table)
    context["ti"].xcom_push(key="transformed", value=df_tmp)


# Функция для выгрузки данных
def load(file_path: str, **context) -> None:
    df = context["ti"].xcom_pull(key="transformed", task_ids=["transform"])[0]
    df.to_csv(file_path, index=None, mode="a")


task_extract = PythonOperator(
    task_id="extract",
    python_callable=extract,
    op_kwargs={"file_path": "dags/profit_table.csv"},
    dag=dag,
    provide_context=True,
)

task_transform = PythonOperator(
    task_id="transform", python_callable=transform_profit, dag=dag, provide_context=True
)

task_load = PythonOperator(
    task_id="load",
    python_callable=load,
    op_kwargs={"file_path": "dags/flags_activity.csv"},
    dag=dag,
    provide_context=True,
)

task_extract >> task_transform >> task_load
