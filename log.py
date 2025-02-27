import logging
import pandas as pd
# from sqlalchemy import text
# from sqlalchemy.exc import SQLAlchemyError

import psutil
import uuid

from datasource import *

log_clickhouse_client = clickhouse_connect.get_client(**clickhouse_conn_params)

def run_query_in_clickhouse(query, clickhouse_client):
    # print(query)
    try:
        result = clickhouse_client.command(query)
        return result
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

def generate_session_id():
    return str(uuid.uuid4())

session_id = generate_session_id();

# Setup Logging Function
def log(created_at, session_id, levelname, filename, funcName, app, database, query, error, message, calc_duration):

    cpu_usage = psutil.cpu_percent(interval=1)
    memory_usage = psutil.virtual_memory().percent
    disk_usage = psutil.disk_usage('/').percent
    query = query.replace("'", "''")
    
    log_query = '''
        INSERT INTO log_commands 
        (created_at, session_id, levelname, filename, funcName, app, database, query, error, message, calc_duration, cpu_usage, memory_usage, disk_usage)
        VALUES 
        ('{created_at}', '{session_id}', '{levelname}', '{filename}', '{funcName}', '{app}', '{database}', '{query}', '{error}', '{message}', {calc_duration}, {cpu_usage}, {memory_usage}, {disk_usage})
        '''.format(created_at = created_at, 
               session_id = session_id, 
               levelname = levelname, 
               filename = filename, 
               funcName = funcName, 
               app = app, 
               database = database,
               query = query,
               error = error,
               message = message,
               calc_duration = calc_duration,
               cpu_usage = cpu_usage,
               memory_usage = memory_usage,
               disk_usage = disk_usage,
               )
    run_query_in_clickhouse(log_query, log_clickhouse_client)

# Configure logging to use clichouse
class SQLHandler(logging.Handler):
    def emit(self, record):
        created_at = pd.to_datetime('now')  # Convert Unix timestamp to pandas datetime
        log(created_at, 
            session_id, 
            record.levelname, 
            record.filename, 
            record.funcName,
            getattr(record, 'app', ''), 
            getattr(record, 'database', ''), 
            getattr(record, 'query', ''), 
            getattr(record, 'error', ''), 
            record.getMessage(),
            getattr(record, 'calc_duration', '')
            )

class ExcludeHttpClientFilter(logging.Filter):
    def filter(self, record):
        # Exclude records whose filename is httpclient.py
        return not (record.filename and record.filename.endswith("httpclient.py"))

# Add MySQL handler to root logger
sql_handler = SQLHandler()
sql_handler.addFilter(ExcludeHttpClientFilter())
logging.getLogger().addHandler(sql_handler)
logger = logging.getLogger()
# Add the filter to your logger or to a specific handler
logger.setLevel(logging.INFO)
