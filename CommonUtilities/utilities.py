import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import logging
import pytest

# Configure the logging
logging.basicConfig(
    filename='logs/etlprocess.log',  # Name of the log file
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

# common function to test file to db as input
def file_to_db_verify(file_path,table_name,db_engine,file_type):
    if file_type == 'csv':
        df_expected = pd.read_csv(file_path)
    elif file_type == 'xml':
        df_expected = pd.read_xml(file_path, xpath='.//item')
    elif file_type == 'json':
        df_expected = pd.read_json(file_path)
    else:
        raise ValueError(f"Unsupported file type passed {file_type}")

    query = f"select * from {table_name}"
    df_actual = pd.read_sql(query, db_engine)
    assert df_actual.equals(df_expected),f"Data extraction failed to load in {table_name}"

# common function to test db to db
def db_to_db_verify(query1,db_engine1,query2,db_engine2):
    df_expected = pd.read_sql(query1, db_engine1)
    df_actual = pd.read_sql(query2, db_engine2)
    assert df_actual.equals(df_expected),f"Data comparision failed"


