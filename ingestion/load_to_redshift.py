"""
ingestion/load_to_redshift.py

Loads raw CSV files from S3 into Redshift staging schema using
COPY command (most efficient method for Redshift bulk loads).

Usage:
    python ingestion/load_to_redshift.py --date 2024-01-15
"""

import logging
import os
from dataclasses import dataclass
from datetime import date
from typing import Optional

import boto3
import redshift_connector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("redshift_loader")

# CONFIG
S3_BUCKET = os.environ.get("S3_RAW_BUCKET", "ecom-pipeline-raw-dev")
REDSHIFT_CONFIG = {
    "host": os.environ["REDSHIFT_HOST"],
    "port": int(os.environ.get("REDSHIFT_PORT", 5439)),
    "database": os.environ.get("REDSHIFT_DATABASE", "ecom_db"),
    "user": os.environ["REDSHIFT_USER"],
    "password": os.environ["REDSHIFT_PASSWORD"],
}
STAGING_SCHEMA = "staging"
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-2")
IAM_ROLE_ARN = os.environ.get("REDSHIFT_IAM_ROLE_ARN", "")  # preferred over keys


@dataclass
class TableSpec:
    """Defines how each CSV maps to a Redshift table."""
    csv_file: str
    table_name: str
    ddl: str
    sort_key: Optional[str] = None
    dist_key: Optional[str] = None

# TABLE DEFINITIONS (DDL)
TABLE_SPECS = [
    TableSpec(
        csv_file="olist_customers_dataset.csv",
        table_name="raw_customers",
        sort_key="customer_id",
        dist_key="customer_id",
        ddl="""
        CREATE TABLE IF NOT EXISTS {schema}.raw_customers (
            customer_id        VARCHAR(64)  NOT NULL,
            customer_unique_id VARCHAR(64),
            customer_zip_code  VARCHAR(10),
            customer_city      VARCHAR(100),
            customer_state     CHAR(2),
            _loaded_at         TIMESTAMP    DEFAULT GETDATE()
        )
        DISTSTYLE KEY DISTKEY(customer_id)
        SORTKEY(customer_id);
        """,
    ),
    TableSpec(
        csv_file="olist_orders_dataset.csv",
        table_name="raw_orders",
        sort_key="order_purchase_timestamp",
        dist_key="order_id",
        ddl="""
        CREATE TABLE IF NOT EXISTS {schema}.raw_orders (
            order_id                    VARCHAR(64)  NOT NULL,
            customer_id                 VARCHAR(64),
            order_status                VARCHAR(50),
            order_purchase_timestamp    TIMESTAMP,
            order_approved_at           TIMESTAMP,
            order_delivered_carrier_date TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP,
            _loaded_at                  TIMESTAMP    DEFAULT GETDATE()
        )
        DISTSTYLE KEY DISTKEY(order_id)
        SORTKEY(order_purchase_timestamp);
        """,
    ),
    TableSpec(
        csv_file="olist_order_items_dataset.csv",
        table_name="raw_order_items",
        dist_key="order_id",
        ddl="""
        CREATE TABLE IF NOT EXISTS {schema}.raw_order_items (
            order_id           VARCHAR(64)  NOT NULL,
            order_item_id      INTEGER,
            product_id         VARCHAR(64),
            seller_id          VARCHAR(64),
            shipping_limit_date TIMESTAMP,
            price              DECIMAL(10,2),
            freight_value      DECIMAL(10,2),
            _loaded_at         TIMESTAMP    DEFAULT GETDATE()
        )
        DISTSTYLE KEY DISTKEY(order_id);
        """,
    ),
    TableSpec(
        csv_file="olist_order_payments_dataset.csv",
        table_name="raw_order_payments",
        dist_key="order_id",
        ddl="""
        CREATE TABLE IF NOT EXISTS {schema}.raw_order_payments (
            order_id             VARCHAR(64),
            payment_sequential   INTEGER,
            payment_type         VARCHAR(30),
            payment_installments INTEGER,
            payment_value        DECIMAL(10,2),
            _loaded_at           TIMESTAMP    DEFAULT GETDATE()
        )
        DISTSTYLE KEY DISTKEY(order_id);
        """,
    ),
    TableSpec(
        csv_file="olist_order_reviews_dataset.csv",
        table_name="raw_order_reviews",
        dist_key="order_id",
        ddl="""
        CREATE TABLE IF NOT EXISTS {schema}.raw_order_reviews (
            review_id            VARCHAR(64),
            order_id             VARCHAR(64),
            review_score         SMALLINT,
            review_comment_title VARCHAR(200),
            review_comment_message VARCHAR(1000),
            review_creation_date TIMESTAMP,
            review_answer_timestamp TIMESTAMP,
            _loaded_at           TIMESTAMP    DEFAULT GETDATE()
        )
        DISTSTYLE KEY DISTKEY(order_id);
        """,
    ),
    TableSpec(
        csv_file="olist_products_dataset.csv",
        table_name="raw_products",
        ddl="""
        CREATE TABLE IF NOT EXISTS {schema}.raw_products (
            product_id                 VARCHAR(64)  NOT NULL,
            product_category_name      VARCHAR(100),
            product_name_length        SMALLINT,
            product_description_length INTEGER,
            product_photos_qty         SMALLINT,
            product_weight_g           DECIMAL(10,2),
            product_length_cm          DECIMAL(10,2),
            product_height_cm          DECIMAL(10,2),
            product_width_cm           DECIMAL(10,2),
            _loaded_at                 TIMESTAMP    DEFAULT GETDATE()
        )
        DISTSTYLE ALL;
        """,
    ),
    TableSpec(
        csv_file="olist_sellers_dataset.csv",
        table_name="raw_sellers",
        ddl="""
        CREATE TABLE IF NOT EXISTS {schema}.raw_sellers (
            seller_id       VARCHAR(64)  NOT NULL,
            seller_zip_code VARCHAR(10),
            seller_city     VARCHAR(100),
            seller_state    CHAR(2),
            _loaded_at      TIMESTAMP    DEFAULT GETDATE()
        )
        DISTSTYLE ALL;
        """,
    ),
    TableSpec(
        csv_file="product_category_name_translation.csv",
        table_name="raw_category_translation",
        ddl="""
        CREATE TABLE IF NOT EXISTS {schema}.raw_category_translation (
            product_category_name         VARCHAR(100),
            product_category_name_english VARCHAR(100),
            _loaded_at                    TIMESTAMP    DEFAULT GETDATE()
        )
        DISTSTYLE ALL;
        """,
    ),
]
 
# CORE LOGIC

def get_redshift_connection():
    return redshift_connector.connect(**REDSHIFT_CONFIG)


def create_schema_if_not_exists(cursor, schema: str):
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    logger.info(f"Schema '{schema}' ensured.")


def truncate_and_load(
    cursor,
    spec: TableSpec,
    s3_prefix: str,
    schema: str = STAGING_SCHEMA,
):
    """
    TRUNCATE the staging table and COPY from S3.
    Staging tables are fully refreshed each run (not incremental).
    """
    table = f"{schema}.{spec.table_name}"
    s3_path = f"s3://{S3_BUCKET}/{s3_prefix}{spec.csv_file}"

    # Create table
    ddl = spec.ddl.format(schema=schema)
    cursor.execute(ddl)

    # Truncate
    cursor.execute(f"TRUNCATE TABLE {table};")
    logger.info(f"  ✓ Truncated {table}")

    # COPY from S3
    copy_sql = f"""
    COPY {table}
    FROM '{s3_path}'
    {_auth_clause()}
    REGION '{AWS_REGION}'
    CSV
    IGNOREHEADER 1
    TIMEFORMAT 'auto'
    BLANKSASNULL
    EMPTYASNULL
    MAXERROR 10;
    """
    cursor.execute(copy_sql)
    logger.info(f"  ↑ COPY complete: {s3_path} → {table}")


def _auth_clause() -> str:
    """Use IAM role if available, fallback to access keys."""
    if IAM_ROLE_ARN:
        return f"IAM_ROLE '{IAM_ROLE_ARN}'"
    return (
        f"ACCESS_KEY_ID '{os.environ['AWS_ACCESS_KEY_ID']}' "
        f"SECRET_ACCESS_KEY '{os.environ['AWS_SECRET_ACCESS_KEY']}'"
    )


def load_all(run_date: date):
    year = run_date.strftime("%Y")
    month = run_date.strftime("%m")
    day = run_date.strftime("%d")
    s3_prefix = f"olist/year={year}/month={month}/day={day}/"

    logger.info(f"Loading from s3://{S3_BUCKET}/{s3_prefix}")

    conn = get_redshift_connection()
    conn.autocommit = False
    cursor = conn.cursor()

    try:
        create_schema_if_not_exists(cursor, STAGING_SCHEMA)

        for spec in TABLE_SPECS:
            logger.info(f"Loading {spec.table_name}...")
            truncate_and_load(cursor, spec, s3_prefix)

        conn.commit()
        logger.info("All tables loaded successfully. Transaction committed.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Load failed, transaction rolled back: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().isoformat())
    args = parser.parse_args()
    load_all(date.fromisoformat(args.date))
