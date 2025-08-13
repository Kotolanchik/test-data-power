import logging
from contextlib import contextmanager
from airflow.providers.postgres.hooks.postgres import PostgresHook

class DBManager:
    def __init__(self, postgres_conn_id='postgres_default'):
        self.conn_id = postgres_conn_id
        self._init_db()

    def _table_exists(self, table_name):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        sql = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)"
        return hook.get_first(sql, (table_name,))[0]

    def _init_db(self):
        tables = {
            'log_batch': """
                CREATE TABLE IF NOT EXISTS log_batch (
                    batch_id TEXT PRIMARY KEY,
                    description TEXT NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    status TEXT NOT NULL CHECK(status IN ('running', 'success', 'failed'))
                )
            """,
            'log_session': """
                CREATE TABLE IF NOT EXISTS log_session (
                    session_id TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL REFERENCES log_batch(batch_id),
                    description TEXT NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    status TEXT NOT NULL CHECK(status IN ('running', 'success', 'failed'))
                )
            """,
            'log_job': """
                CREATE TABLE IF NOT EXISTS log_job (
                    job_id SERIAL PRIMARY KEY,
                    session_id TEXT NOT NULL REFERENCES log_session(session_id),
                    description TEXT NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    status TEXT NOT NULL CHECK(status IN ('running', 'success', 'failed', 'partial_success')),
                    rows_processed INTEGER,
                    rows_inserted INTEGER,
                    duplicates_skipped INTEGER
                )
            """,
            '': """
             CREATE TABLE IF NOT EXISTS log_error (
                    error_id SERIAL PRIMARY KEY,
                    session_id TEXT NOT NULL REFERENCES log_session(session_id),
                    job_id INTEGER REFERENCES log_job(job_id),
                    error_time TIMESTAMP NOT NULL,
                    error_message TEXT NOT NULL,
                    stack_trace TEXT,
                    context TEXT
                )
            """
            ,
            'ods_ozon_orders': """
                CREATE TABLE IF NOT EXISTS ods_ozon_orders (
                    order_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    date TIMESTAMP NOT NULL,
                    amount REAL NOT NULL,
                    customer_id TEXT NOT NULL,
                    customer_region TEXT NOT NULL,
                    source_session_id TEXT NOT NULL REFERENCES log_session(session_id),
                    source_file_path TEXT NOT NULL,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed BOOLEAN DEFAULT FALSE
                )
            """,
            'dds_regions': """
                CREATE TABLE IF NOT EXISTS dds_regions (
                    region_key SERIAL PRIMARY KEY,
                    region_name TEXT NOT NULL,
                    source_batch_id TEXT NOT NULL REFERENCES log_batch(batch_id),
                    start_date TIMESTAMP NOT NULL,
                    end_date TIMESTAMP,
                    is_current BOOLEAN DEFAULT TRUE
                )
            """,
            'dds_customers': """
                CREATE TABLE IF NOT EXISTS dds_customers (
                    customer_key SERIAL PRIMARY KEY,
                    customer_id TEXT NOT NULL,
                    region_key INTEGER NOT NULL REFERENCES dds_regions(region_key),
                    source_batch_id TEXT NOT NULL REFERENCES log_batch(batch_id),
                    start_date TIMESTAMP NOT NULL,
                    end_date TIMESTAMP,
                    is_current BOOLEAN DEFAULT TRUE
                )
            """,
            'fct_orders': """
                CREATE TABLE IF NOT EXISTS fct_orders (
                    order_key SERIAL PRIMARY KEY,
                    order_id TEXT NOT NULL,
                    customer_key INTEGER NOT NULL REFERENCES dds_customers(customer_key),
                    status TEXT NOT NULL,
                    date DATE NOT NULL,
                    amount REAL NOT NULL,
                    source_batch_id TEXT NOT NULL REFERENCES log_batch(batch_id),
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        }

        hook = PostgresHook(postgres_conn_id=self.conn_id)
        for table_name, table_ddl in tables.items():
            if not self._table_exists(table_name):
                hook.run(table_ddl)
                logging.info(f"Created table: {table_name}")

    @contextmanager
    def _get_cursor(self, commit=False):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            yield cursor
            if commit:
                conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(f"Database error: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()

    def execute(self, query, params=None, commit=False):
        with self._get_cursor(commit) as cursor:
            cursor.execute(query, params or ())
            return cursor

    def executemany(self, query, params_list, commit=False):
        with self._get_cursor(commit) as cursor:
            cursor.executemany(query, params_list)
            return cursor

    def fetch_one(self, query, params=None):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        return hook.get_first(query, parameters=params)

    def fetch_all(self, query, params=None):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        return hook.get_records(query, parameters=params)

    def fetch_existing_ods_orders(self, order_ids):
        if not order_ids:
            return set()

        hook = PostgresHook(postgres_conn_id=self.conn_id)
        chunk_size = 500
        existing_orders = set()

        for i in range(0, len(order_ids), chunk_size):
            chunk = order_ids[i:i + chunk_size]
            query = "SELECT order_id FROM ods_ozon_orders WHERE order_id = ANY(%s)"
            records = hook.get_records(query, parameters=(chunk,))
            existing_orders.update(row[0] for row in records)

        return existing_orders