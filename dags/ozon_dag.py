from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_processor import DataProcessor
from data_migrator import DataMigrator

POSTGRES_CONN_ID = 'postgres_default'
DATA_FILE = '/data/ozon_orders.json'

DEFAULT_ARGS = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
}

def start_batch(**context):
    processor = DataProcessor(POSTGRES_CONN_ID)
    batch_id = context['run_id']
    return processor.batch_logger.start_batch(batch_id, "Ozon daily processing")

def start_session_ods(**context):
    processor = DataProcessor(POSTGRES_CONN_ID)
    batch_id = context['ti'].xcom_pull(task_ids='start_batch')
    return processor.session_logger.start_session(batch_id, "ODS data loading")

def start_session_dds(**context):
    processor = DataProcessor(POSTGRES_CONN_ID)
    batch_id = context['ti'].xcom_pull(task_ids='start_batch')
    return processor.session_logger.start_session(batch_id, "DDS data migration")

def load_ods(**context):
    processor = DataProcessor(POSTGRES_CONN_ID)
    batch_id = context['ti'].xcom_pull(task_ids='start_batch')
    session_id = context['ti'].xcom_pull(task_ids='start_session_ods')
    return processor.process_orders(DATA_FILE, batch_id, session_id)

def migrate_dds(**context):
    migrator = DataMigrator(POSTGRES_CONN_ID)
    batch_id = context['ti'].xcom_pull(task_ids='start_batch')
    session_id = context['ti'].xcom_pull(task_ids='start_session_dds')
    return migrator.migrate_to_dds(batch_id, session_id)

def end_session_ods(**context):
    processor = DataProcessor(POSTGRES_CONN_ID)
    session_id = context['ti'].xcom_pull(task_ids='start_session_ods')
    success = context['ti'].xcom_pull(task_ids='load_ods') is not None
    status = 'success' if success else 'failed'
    processor.session_logger.end_session(session_id, status)
    return status

def end_session_dds(**context):
    processor = DataProcessor(POSTGRES_CONN_ID)
    session_id = context['ti'].xcom_pull(task_ids='start_session_dds')
    success = context['ti'].xcom_pull(task_ids='migrate_to_dds') is not None
    status = 'success' if success else 'failed'
    processor.session_logger.end_session(session_id, status)
    return status

def end_batch(**context):
    processor = DataProcessor(POSTGRES_CONN_ID)
    batch_id = context['ti'].xcom_pull(task_ids='start_batch')
    ods_status = context['ti'].xcom_pull(task_ids='end_session_ods')
    dds_status = context['ti'].xcom_pull(task_ids='end_session_dds')

    status = 'success' if ods_status == 'success' and dds_status == 'success' else 'failed'
    processor.batch_logger.end_batch(batch_id, status)
    return status

with DAG(
        'ozon_data_pipeline',
        default_args=DEFAULT_ARGS,
        start_date=datetime(2024, 1, 1),
        schedule_interval='@daily',
        catchup=False,
        max_active_runs=1,
        tags=['ozon', 'etl']
) as dag:
    start_batch_task = PythonOperator(
        task_id='start_batch',
        python_callable=start_batch,
        provide_context=True
    )

    start_session_ods_task = PythonOperator(
        task_id='start_session_ods',
        python_callable=start_session_ods,
        provide_context=True
    )

    load_ods_task = PythonOperator(
        task_id='load_ods',
        python_callable=load_ods,
        provide_context=True
    )

    end_session_ods_task = PythonOperator(
        task_id='end_session_ods',
        python_callable=end_session_ods,
        provide_context=True,
        trigger_rule='all_done'
    )

    start_session_dds_task = PythonOperator(
        task_id='start_session_dds',
        python_callable=start_session_dds,
        provide_context=True
    )

    migrate_dds_task = PythonOperator(
        task_id='migrate_to_dds',
        python_callable=migrate_dds,
        provide_context=True
    )

    end_session_dds_task = PythonOperator(
        task_id='end_session_dds',
        python_callable=end_session_dds,
        provide_context=True,
        trigger_rule='all_done'
    )

    end_batch_task = PythonOperator(
        task_id='end_batch',
        python_callable=end_batch,
        provide_context=True,
        trigger_rule='all_done'
    )

    start_batch_task >> start_session_ods_task >> load_ods_task >> end_session_ods_task
    end_session_ods_task >> start_session_dds_task >> migrate_dds_task >> end_session_dds_task
    [end_session_ods_task, end_session_dds_task] >> end_batch_task