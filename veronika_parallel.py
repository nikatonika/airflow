import pandas as pd
from datetime import datetime, date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from scripts.transform_script_parallel import transfrom

product_list = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]

dag = DAG(
    "monthly_activity_dag_VeronikaZaslavskaia_parallel",
    schedule="0 0 5 * *",
    start_date=datetime(2024, 4, 30),
    max_active_tasks=10,
)

# Функция для загрузки данных
# Логичнее простенькую предобработку сделать тут, чтобы потом не делать одно и то же
# Но много раз и в параллель
def extract(file_path: str, **context) -> None:
    df = pd.read_csv(file_path)

    start_date = pd.to_datetime(date.today()) - pd.DateOffset(months=2)
    end_date = pd.to_datetime(date.today()) + pd.DateOffset(months=1)
    date_list = pd.date_range(start=start_date, end=end_date, freq="M").strftime(
        "%Y-%m-01"
    )

    df_tmp = df[df["date"].isin(date_list)].drop("date", axis=1).groupby("id").sum()
    context["ti"].xcom_push(key="extracted", value=df_tmp)


# Функция для преобразования данных
def transform_profit(product: str, **context) -> None:
    profit_table = context["ti"].xcom_pull(key="extracted", task_ids=["extract"])[0]
    df_tmp = transfrom(df_tmp=profit_table, product=product)
    context["ti"].xcom_push(key=f"transformed_{product}", value=df_tmp)


# Функция для выгрузки данных
def load(file_path: str, **context) -> None:
    for j, i in enumerate(product_list):
        if j == 0:
            df = context["ti"].xcom_pull(
                key=f"transformed_{i}", task_ids=[f"transform_{i}"]
            )[0]
        else:
            df[f"flag_{i}"] = context["ti"].xcom_pull(
                key=f"transformed_{i}", task_ids=[f"transform_{i}"]
            )[0][f"flag_{i}"]

    df.to_csv(file_path, index=None, mode="a")


task_extract = PythonOperator(
    task_id="extract",
    python_callable=extract,
    op_kwargs={"file_path": "/home/markblumenau/airflow/dags/profit_table.csv"},
    dag=dag,
    provide_context=True,
)

task_transform = []
for i in product_list:
    task_transform_profit = PythonOperator(
        task_id=f"transform_{i}",
        python_callable=transform_profit,
        op_kwargs={"product": i},
        dag=dag,
        provide_context=True,
    )
    task_transform.append(task_transform_profit)


task_load = PythonOperator(
    task_id="load",
    python_callable=load,
    op_kwargs={"file_path": "/home/markblumenau/airflow/dags/flags_activity.csv"},
    dag=dag,
    provide_context=True,
)

task_extract >> task_transform >> task_load
