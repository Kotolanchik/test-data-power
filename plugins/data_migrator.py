import logging
from datetime import datetime
from db_manager import DBManager
from logger import JobLogger, ErrorLogger


class DataMigrator:
    def __init__(self, db_params):
        self.db = DBManager(db_params)
        self.job_logger = JobLogger(self.db)
        self.error_logger = ErrorLogger(self.db)

    def migrate_to_dds(self, batch_id, session_id):
        job_id = self.job_logger.start_job(
            session_id,
            f"Migration for batch {batch_id}"
        )

        try:
            batch_time = self._get_batch_time(batch_id)
            regions = self._process_regions(batch_id, batch_time, job_id, session_id)
            customers = self._process_customers(batch_id, batch_time, job_id, session_id)
            orders = self._load_orders(batch_id, job_id, session_id)
            self._mark_processed(batch_id)

            self.job_logger.end_job(
                job_id,
                'success',
                rows_processed=regions + customers + orders
            )

            logging.info(f"DDS: Regions={regions} | Customers={customers} | Orders={orders}")

            return orders
        except Exception as e:
            self.error_logger.log_error(session_id, job_id, e)
            self.job_logger.end_job(job_id, 'failed')
            raise

    def _get_batch_time(self, batch_id):
        row = self.db.fetch_one(
            "SELECT start_time FROM log_batch WHERE batch_id = %s",
            (batch_id,)
        )
        return row[0] if row else datetime.now()

    def _process_regions(self, batch_id, batch_time, job_id, session_id):
        regions = self.db.fetch_all(
            "SELECT DISTINCT customer_region FROM ods_ozon_orders WHERE processed = FALSE"
        )
        if not regions:
            return 0

        count = 0
        for row in regions:
            region = row[0]
            try:
                exists = self.db.fetch_one(
                    "SELECT region_key FROM dds_regions WHERE region_name = %s AND is_current = TRUE",
                    (region,)
                )
                if not exists:
                    self.db.execute(
                        "INSERT INTO dds_regions (region_name, source_batch_id, start_date) VALUES (%s, %s, %s)",
                        (region, batch_id, batch_time),
                        commit=True
                    )
                    count += 1
            except Exception as e:
                self.error_logger.log_error(
                    session_id,
                    job_id,
                    f"Region error: {e}",
                    {"region": region}
                )
        return count

    def _process_customers(self, batch_id, batch_time, job_id, session_id):
        customers = self.db.fetch_all("""
            SELECT DISTINCT o.customer_id, o.customer_region, r.region_key
            FROM ods_ozon_orders o
            JOIN dds_regions r ON o.customer_region = r.region_name AND r.is_current = TRUE
            WHERE o.processed = FALSE
        """)
        if not customers:
            return 0

        count = 0
        for row in customers:
            customer_id = row[0]
            region_key = row[2]  # region_key - третий элемент в результате
            try:
                current = self.db.fetch_one(
                    "SELECT customer_key, region_key FROM dds_customers WHERE customer_id = %s AND is_current = TRUE",
                    (customer_id,)
                )

                if not current:
                    self.db.execute(
                        "INSERT INTO dds_customers (customer_id, region_key, source_batch_id, start_date) VALUES (%s, %s, %s, %s)",
                        (customer_id, region_key, batch_id, batch_time),
                        commit=True
                    )
                    count += 1
                elif current[1] != region_key:  # region_key - второй элемент в current
                    self.db.execute(
                        "UPDATE dds_customers SET end_date = %s, is_current = FALSE WHERE customer_key = %s",
                        (batch_time, current[0]),
                        commit=True
                    )
                    self.db.execute(
                        "INSERT INTO dds_customers (customer_id, region_key, source_batch_id, start_date) VALUES (%s, %s, %s, %s)",
                        (customer_id, region_key, batch_id, batch_time),
                        commit=True
                    )
                    count += 1
            except Exception as e:
                self.error_logger.log_error(
                    session_id,
                    job_id,
                    f"Customer error: {e}",
                    {"customer_id": customer_id}
                )
        return count

    def _load_orders(self, batch_id, job_id, session_id):
        orders = self.db.fetch_all("""
            SELECT o.order_id, o.status, o.date::date, o.amount, c.customer_key
            FROM ods_ozon_orders o
            JOIN dds_customers c ON o.customer_id = c.customer_id AND c.is_current = TRUE
            WHERE o.processed = FALSE AND o.source_session_id IN (
                SELECT session_id FROM log_session WHERE batch_id = %s
            )
        """, (batch_id,))

        if not orders:
            return 0

        data = []
        for row in orders:
            try:
                data.append((
                    row[0],  # order_id
                    row[4],  # customer_key
                    row[1],  # status
                    row[2],  # date (уже DATE)
                    row[3],  # amount
                    batch_id
                ))
            except Exception as e:
                self.error_logger.log_error(
                    session_id,
                    job_id,
                    f"Order error: {e}",
                    {"order_id": row[0] if len(row) > 0 else 'unknown'}
                )

        if data:
            self.db.executemany(
                "INSERT INTO fct_orders (order_id, customer_key, status, date, amount, source_batch_id) VALUES (%s, %s, %s, %s, %s, %s)",
                data,
                commit=True
            )
        return len(data)

    def _mark_processed(self, batch_id):
        self.db.execute(
            "UPDATE ods_ozon_orders SET processed = TRUE WHERE source_session_id IN (SELECT session_id FROM log_session WHERE batch_id = %s)",
            (batch_id,),
            commit=True
        )