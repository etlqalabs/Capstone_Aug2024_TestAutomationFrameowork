# assignment:
# complete all the exception handling for all the test case



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
# Create Oracle engine
#oracle_engine = create_engine('oracle+cx_oracle://system:admin@localhost:1521/xe')
oracle_engine = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}')

logger.info("My data extraction test cases started ...........")
@pytest.mark.smoke
def test_extraction_SRC_sales_data_to_Staging_sales():
    try:
        logger.info("Data extrcation from sales_data.csv to staging_sales table has been initiated")
        file_to_db_verify("TestData/sales_data.csv","staging_sales", mysql_engine, "csv")
        logger.info("Data extrcation from sales_data.csv to staging_sales table has been completed")
    except Exception as e:
        logger.error(f"error during data extration:{e}")
        pytest.fail(f"Test failed due to an error:{e}")


@pytest.mark.smoke
def test_extraction_SRCProduct_data_to_Staging_products():
    try:
        logger.info("Data extrcation test from product_data.csv to staging_product table has been initiated")
        file_to_db_verify("TestData/product1_data.csv","staging_product", mysql_engine, "csv")
        logger.info("Data extrcation test from product_data.csv to staging_product table has been completed")
    except Exception as e:
        logger.error(f"error during data extration:{e}")
        pytest.fail(f"Test failed due to an error:{e}")

@pytest.mark.regression
def test_extraction_SRCSupplier_data_to_Staging_Supplier():
    try:
        logger.info("Data extrcation test from supplier_data.json to staging_product table has been initiated")
        file_to_db_verify("TestData/supplier_data.json", "staging_supplier", mysql_engine, "json")
        logger.info("Data extrcation test from supplier_data.json to staging_supplier table has been completed")
    except Exception as e:
        logger.error(f"error during data extration:{e}")
        pytest.fail(f"Test failed due to an error:{e}")



@pytest.mark.regression
def test_extraction_SRCInventory_data_to_Staging_Inventory():
    file_to_db_verify("TestData/inventory_data.xml", "staging_inventory", mysql_engine, "xml")

@pytest.mark.regression
@pytest.mark.smoke
def test_extraction_SRCOracle_to_mySQL():
    try:
        query1 = """select * from stores"""
        query2 = """select * from staging_stores"""
        db_to_db_verify(query1, oracle_engine, query2, mysql_engine)
    except Exception as e:
        logger.error(f"error during data extration:{e}")
        pytest.fail(f"Test failed due to an error:{e}")











'''
def test_extraction_SRC_sales_data_to_Staging_sales():
    df_expected = pd.read_csv('TestData/sales_data.csv')
    query = ("""SELECT * FROM staging_sales""")
    df_actual = pd.read_sql(query, mysql_engine)
    assert df_actual.equals(df_expected),"Data extraction failed for sales_data.csv"

def test_extraction_SRCProduct_data_to_Staging_products():
    df_expected = pd.read_csv('TestData/product_data.csv')
    query = ("""SELECT * FROM staging_product""")
    df_actual = pd.read_sql(query, mysql_engine)
    assert df_actual.equals(df_expected),"Data extraction failed for product_data.csv"

def test_extraction_SRCSupplier_data_to_Staging_Supplier():
    df_expected = pd.read_json('TestData/supplier_data.json')
    query = ("""SELECT * FROM staging_supplier""")
    df_actual = pd.read_sql(query, mysql_engine)
    assert df_actual.equals(df_expected),"Data extraction failed for supplier_data.json"

def test_extraction_SRCInventory_data_to_Staging_Inventory():
    df_expected = pd.read_xml('TestData/inventory_data.xml',xpath='.//item')
    query = ("""SELECT * FROM staging_inventory""")
    df_actual = pd.read_sql(query, mysql_engine)
    assert df_actual.equals(df_expected),"Data extraction failed for inventoryr_data.xml"
'''

logger.info("My data extraction test cases completed ...........")