import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import logging
import pytest

# Configure the logging
from CommonUtilities.utilities import file_to_db_verify, db_to_db_verify

logging.basicConfig(
    filename='Logs/etlprocess.log',  # Name of the log file
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)

# create mysql database commection
from CommonUtilities.config import *

#mysql_engine = create_engine('mysql+pymysql://root:Admin%40143@localhost:3308/retaildwh')
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')





def test_count_check_between_sales_with_details_AND_fact_sales():
    try:
        logger.info("test_fact_sales_table_load has been initiated ....")
        expected_query = """SELECT
        count(*)
        FROM sales_with_details"""
        actual_query = """SELECT
                count(*)
                FROM fact_sales"""
        db_to_db_verify(expected_query, mysql_engine, actual_query, mysql_engine)
        logger.info("test_fact_sales_table_load has been completed ....")
    except Exception as e:
        logger.error(f"error during data extration:{e}")
        pytest.fail(f"Test failed due to an error:{e}")


@pytest.mark.skip
def test_fact_sales_table_load():
    try:
        logger.info("test_fact_sales_table_load has been initiated ....")
        expected_query = """SELECT
        sales_id,
        product_id,
        store_id,
        quantity,
        sale_date
        FROM sales_with_details"""
        actual_query = """SELECT
                sales_id,
                product_id,
                store_id,
                quantity,
                sale_date
                FROM fact_sales"""
        db_to_db_verify(expected_query, mysql_engine, actual_query, mysql_engine)
        logger.info("test_fact_sales_table_load has been completed ....")
    except Exception as e:
        logger.error(f"error during data extration:{e}")
        pytest.fail(f"Test failed due to an error:{e}")

     # Assignment to omplete all the other 3 target load test cases
