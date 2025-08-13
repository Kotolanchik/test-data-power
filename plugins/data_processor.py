import json
import logging
import os
from datetime import datetime
from db_manager import DBManager
from logger import BatchLogger, SessionLogger, JobLogger, ErrorLogger

class DataProcessor:
    def __init__(self, postgres_conn_id='postgres_default'):
        self.db = DBManager(postgres_conn_id)
        self.batch_logger = BatchLogger(self.db)
        self.session_logger = SessionLogger(self.db)
        self.job_logger = JobLogger(self.db)
        self.error_logger = ErrorLogger(self.db)

    def process_orders(self, file_path, batch_id, session_id):
        job_id = self.job_logger.start_job(
            session_id,
            f"Processing: {file_path}"
        )

        try:
            with open(file_path, 'r') as f:
                orders = json.load(f)

            abs_path = os.path.abspath(file_path)
            all_ids = [order['order_id'] for order in orders]
            existing = self.db.fetch_existing_ods_orders(all_ids)

            processed = []
            errors = 0
            duplicates = 0

            for order in orders:
                order_id = order['order_id']

                if order_id in existing:
                    duplicates += 1
                    continue

                try:
                    order_datetime = datetime.fromisoformat(order['date'])
                    processed.append((
                        order_id,
                        order['status'],
                        order_datetime,
                        float(order['amount']),
                        order['customer']['id'],
                        order['customer']['region'],
                        session_id,
                        abs_path
                    ))
                except (KeyError, ValueError) as e:
                    errors += 1
                    self.error_logger.log_error(
                        session_id,
                        job_id,
                        f"Processing error: {e}",
                        order
                    )

            inserted = 0
            if processed:
                self.db.executemany(
                    """INSERT INTO ods_ozon_orders 
                    (order_id, status, date, amount, customer_id, customer_region, 
                     source_session_id, source_file_path) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    processed,
                    commit=True
                )
                inserted = len(processed)

            status = 'success'
            if errors > 0 or duplicates > 0:
                status = 'partial_success' if inserted > 0 else 'failed'

            self.job_logger.end_job(
                job_id,
                status,
                rows_processed=len(orders),
                rows_inserted=inserted,
                duplicates_skipped=duplicates
            )

            logging.info(
                f"ODS: Processed {len(orders)} orders | New: {inserted} | Errors: {errors} | Duplicates: {duplicates}")

            return inserted
        except Exception as e:
            self.error_logger.log_error(session_id, job_id, e)
            self.job_logger.end_job(job_id, 'failed')
            raise