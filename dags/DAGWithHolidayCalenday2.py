Create a holiday calendar;


# holidays.py
import datetime

HOLIDAYS = [
    datetime.date(2025, 3, 14),  # Holi
    datetime.date(2025, 10, 20), # Diwali
    datetime.date(2025, 12, 25), # Christmas
]



from airflow import DAG
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from datetime import datetime, timedelta
from holidays import HOLIDAYS

def _should_run(execution_date, **context):
    run_date = execution_date.date()
    if run_date in HOLIDAYS:
        print(f"Skipping run for holiday: {run_date}")
        return False  # This stops the DAG
    return True

def _process_data(**kwargs):
    print("Processing data...")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='skip_on_holidays',
    default_args=default_args,
    schedule_interval='0 6 * * *',  # daily at 6 AM
    catchup=False,
) as dag:

    check_holiday = ShortCircuitOperator(
        task_id='check_holiday',
        python_callable=_should_run,
        provide_context=True
    )

    process_data = PythonOperator(
        task_id='process_data',
        python_callable=_process_data
    )

    check_holiday >> process_data




