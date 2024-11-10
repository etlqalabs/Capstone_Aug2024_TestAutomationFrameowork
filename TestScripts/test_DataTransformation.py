'''
    Assignment:
    complete 2 other test cases here
    1. Aggergate transformation test case for inventory
    2. Joiner transformation test case

'''

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




def test_filter_transfromation_test():
        try:
            logger.info("Filter transfromation test has been initiated ....")
            query1 = """SELECT * FROM staging_sales where sale_date>='2024-09-05'"""
            query2 = """select * from filtered_sales"""
            db_to_db_verify(query1, mysql_engine, query2, mysql_engine)
            logger.info("Filter transfromation test has been completed ....")
        except Exception as e:
            logger.error(f"error during data extration:{e}")
            pytest.fail(f"Test failed due to an error:{e}")



def test_Router_High_Region_Sales_transfromation_test():
        try:
            logger.info("High region Router transfromation test has been initiated ....")
            query1 = """SELECT * FROM filtered_sales where region='High'"""
            query2 = """select * from high_sales"""
            db_to_db_verify(query1, mysql_engine, query2, mysql_engine)
            logger.info("High region Router transfromation test has been completed ....")
        except Exception as e:
            logger.error(f"error during data extration:{e}")
            pytest.fail(f"Test failed due to an error:{e}")

"""
Test case for testing whether the Low region data is 
correctly routered to the appropriate intermediatory 
table
"""

def test_Router_Low_Region_Sales_transfromation_test():
    try:
        logger.info("Low region Router transfromation test has been initiated ....")
        query1 = """SELECT * FROM filtered_sales where region='Low'"""
        query2 = """select * from low_sales"""
        db_to_db_verify(query1, mysql_engine, query2, mysql_engine)
        logger.info("Low region Router transfromation test has been completed ....")
    except Exception as e:
        logger.error(f"error during data extration:{e}")
        pytest.fail(f"Test failed due to an error:{e}")

    # Aggegeate_sales_data Transfromation checks

def test_aggregate_Sales_data_transfromation_test():
        try:
            logger.info("test_aggregate_Sales_data_transfromation_test has been initiated ....")
            exepcted_query ="""
            SELECT 
            product_id,
            MONTH(sale_date) AS month,
            YEAR(sale_date) AS year,
            SUM(quantity * price) AS total_sales
            FROM filtered_sales
            GROUP BY product_id, MONTH(sale_date), YEAR(sale_date);
            """
            actual_query = """select * from monthly_sales_summary_source"""
            db_to_db_verify(exepcted_query, mysql_engine, actual_query, mysql_engine)
            logger.info("test_aggregate_Sales_data_transfromation_test has been completed ....")
        except Exception as e:
            logger.error(f"error during data extration:{e}")
            pytest.fail(f"Test failed due to an error:{e}")
