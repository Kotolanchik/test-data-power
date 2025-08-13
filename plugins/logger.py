import uuid
import logging
import traceback
from datetime import datetime
from db_manager import DBManager
from airflow.providers.postgres.hooks.postgres import PostgresHook

class BaseLogger:
    def __init__(self, db_manager):
        self.db = db_manager

class BatchLogger(BaseLogger):
    def start_batch(self, batch_id, description):
        self.db.execute(
            "INSERT INTO log_batch (batch_id, description, start_time, status) VALUES (%s, %s, %s, 'running')",
            (batch_id, description, datetime.now()),
            commit=True
        )
        logging.info(f"Batch STARTED: {description} [{batch_id}]")
        return batch_id

    def end_batch(self, batch_id, status):
        self.db.execute(
            "UPDATE log_batch SET end_time = %s, status = %s WHERE batch_id = %s",
            (datetime.now(), status, batch_id),
            commit=True
        )
        logging.info(f"Batch {status.upper()}: {batch_id}")

class SessionLogger(BaseLogger):
    def start_session(self, batch_id, description):
        session_id = f"session_{uuid.uuid4().hex}"
        self.db.execute(
            "INSERT INTO log_session (session_id, batch_id, description, start_time, status) VALUES (%s, %s, %s, %s, 'running')",
            (session_id, batch_id, description, datetime.now()),
            commit=True
        )
        logging.info(f"Session STARTED: {description} [{session_id}]")
        return session_id

    def end_session(self, session_id, status):
        self.db.execute(
            "UPDATE log_session SET end_time = %s, status = %s WHERE session_id = %s",
            (datetime.now(), status, session_id),
            commit=True
        )
        logging.info(f"Session {status.upper()}: {session_id}")

class JobLogger(BaseLogger):
    def start_job(self, session_id, description):
        hook = PostgresHook(postgres_conn_id=self.db.conn_id)
        sql = """
            INSERT INTO log_job (session_id, description, start_time, status)
            VALUES (%s, %s, %s, 'running')
            RETURNING job_id
        """
        job_id = hook.get_first(sql, (session_id, description, datetime.now()))[0]
        logging.info(f"Job STARTED: {description} [job_id={job_id}]")
        return job_id

    def end_job(self, job_id, status, rows_processed=None, rows_inserted=None, duplicates_skipped=None):
        self.db.execute(
            "UPDATE log_job SET end_time = %s, status = %s, rows_processed = %s, rows_inserted = %s, duplicates_skipped = %s WHERE job_id = %s",
            (datetime.now(), status, rows_processed, rows_inserted, duplicates_skipped, job_id),
            commit=True
        )
        logging.info(f"Job {status.upper()}: [job_id={job_id}]")

class ErrorLogger(BaseLogger):
    def log_error(self, session_id, job_id, error, context=None):
        hook = PostgresHook(postgres_conn_id=self.db.conn_id)
        sql = """
            INSERT INTO log_error (session_id, job_id, error_time, error_message, stack_trace, context)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING error_id
        """
        error_id = hook.get_first(
            sql,
            (session_id, job_id, datetime.now(), str(error), traceback.format_exc(), str(context))
        )[0]
        logging.error(f"Error logged: {error_id} - {str(error)}")
        return error_id