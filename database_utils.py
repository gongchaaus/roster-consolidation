import pandas as pd
import json

from sqlalchemy import text
from datasource import *
from log import logger

def read_csv_from_config(gs_config):
    url = f'https://docs.google.com/spreadsheets/d/{gs_config['sheet_id']}/gviz/tq?tqx=out:csv&sheet={gs_config['sheet_name']}'
    # Create the SQLAlchemy engine
    df  = pd.read_csv(url)
    return df

def get_DataFrame_from_mariadb(query, mariadb_engine):   
    start = pd.Timestamp.now()
    extra={
        'query': query,
        'database': mariadb_engine.url,
        }
    try:
        mariadb_df = pd.read_sql(query, mariadb_engine)
    except Exception as e:
        mariadb_df = pd.DataFrame()
        extra['error'] = str(e).replace("'", "''")[:2048].strip("'").strip("'")
        logger.exception("Error executing query", extra=extra)
    finally:
        calc_duration = (pd.Timestamp.now() - start).total_seconds()
        extra['calc_duration']=calc_duration
        message = json.dumps({
            "mariadb_df.shape": mariadb_df.shape,
            })
        logger.debug(message, extra=extra)
    return mariadb_df

def get_DataFrame_from_clickhouse(query, clickhouse_client):
    start = pd.Timestamp.now()
    extra={
        'query': query,
        'database': clickhouse_client.url,
        }
    try:
        clickhouse_df = clickhouse_client.query_df(query)
    except Exception as e:
        clickhouse_df = pd.DataFrame()
        extra['error'] = str(e).replace("'", "''")[:2048].strip("'").strip("'")
        logger.exception("Error executing query", extra=extra)
    finally:
        calc_duration = (pd.Timestamp.now() - start).total_seconds()
        extra['calc_duration']=calc_duration
        message = json.dumps({
            "clickhouse_df.shape": clickhouse_df.shape,
            })
        logger.debug(message, extra=extra)
    return clickhouse_df

def run_query_in_mariadb(query, mariadb_engine):
    start = pd.Timestamp.now()
    extra={
        'query': query,
        'database': mariadb_engine.url,
        }
    try:
        with mariadb_engine.connect() as connection:
            result = connection.execute(text(query))
        return result.fetchall()
    except Exception as e:
        result =  None
        extra['error'] = str(e).replace("'", "''")[:2048].strip("'").strip("'")
        logger.exception("Error executing query", extra=extra)
    finally:
        calc_duration = (pd.Timestamp.now() - start).total_seconds()
        extra['calc_duration']=calc_duration
        logger.debug('', extra=extra)
    return result

def run_query_in_clickhouse(query, clickhouse_client):
    start = pd.Timestamp.now()
    extra={
        'query': query,
        'database': clickhouse_client.url,
        }
    try:
        result = clickhouse_client.command(query)
    except Exception as e:
        # print(f"Error executing query: {e}")
        result =  None
        extra['error'] = str(e).replace("'", "''")[:2048].strip("'").strip("'")
        logger.exception("Error executing query", extra=extra)
    finally:
        calc_duration = (pd.Timestamp.now() - start).total_seconds()
        extra['calc_duration']=calc_duration
        logger.debug('', extra=extra)
    return result

def insert_DataFrameinto_clickhouse(table_name, df, clickhouse_client):
    start = pd.Timestamp.now()
    extra={
        'database': clickhouse_client.url,
        }
    try:
        clickhouse_client.insert_df(table_name, df)
    except Exception as e:
        # print(f"Error executing query: {e}")
        extra['error'] = str(e).replace("'", "''")[:2048].strip("'").strip("'")
        logger.exception("Error executing query", extra=extra)
    finally:
        calc_duration = (pd.Timestamp.now() - start).total_seconds()
        extra['calc_duration']=calc_duration
        message = json.dumps({
            'df.size': df.size,
            'df.dtypes': df.dtypes.astype(str).to_dict(),
            'table_name': table_name
            })
        logger.debug(message, extra=extra)

def migrate_table_schema(table_name, mariadb_engine, clickhouse_client):

    # Get the schema from MariaDB
    mariadb_schema = run_query_in_mariadb(f"DESCRIBE {table_name}", mariadb_engine)

    # Mapping from MariaDB to ClickHouse data types with default values
    type_mapping = {
        'int': ('Int32', '0'),
        'varchar': ('String', "''"),
        'text': ('String', "''"),
        'decimal': ('Decimal(12,4)', '0'),
        'datetime': ('DateTime', "'1970-01-01 00:00:00'"),
        'date': ('Date32', "''1970-01-01''"),
        'time': ('DateTime', "'1970-01-01 00:00:00'"),
        'tinyint': ('Int8', '0'),
        'smallint': ('Int16', '0'),
        'bigint': ('Int64', '0'),
        'float': ('Float32', '0'),
        'double': ('Float64', '0'),
        'timestamp': ('DateTime', "'1970-01-01 00:00:00'"),
        'char': ('String', "''"),
        'boolean': ('UInt8', '0'),
    }

    clickhouse_columns = []
    for column in mariadb_schema:
        col_name, col_type, col_null = column[0], column[1], column[2]
        base_type = col_type.split('(')[0].lower()
        clickhouse_type, default_value = type_mapping.get(base_type, ('String', "''"))
        
        if col_null == 'YES':
            column_def = f"{col_name} Nullable({clickhouse_type}) DEFAULT NULL"
        else:
            column_def = f"{col_name} {clickhouse_type} DEFAULT {default_value}"
        
        clickhouse_columns.append(column_def)

    # Retrieve the primary key column(s) from the source MariaDB table
    primary_keys = run_query_in_mariadb(f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY'", mariadb_engine)
    
    if primary_keys:
        primary_key_columns = [key[4] for key in primary_keys]  # Assuming the column name is in the 5th position
        order_by_clause = f"ORDER BY ({', '.join(primary_key_columns)})"
    else:
        order_by_clause = "ORDER BY tuple()"

    # Construct the CREATE TABLE query for ClickHouse
    create_table_query = f"CREATE TABLE {table_name} ({', '.join(clickhouse_columns)}) ENGINE = MergeTree() {order_by_clause}"

    # Execute the query to create the table in ClickHouse
    run_query_in_clickhouse(create_table_query, clickhouse_client)

    return  mariadb_schema 

def get_primary_keys_from_clickhouse(table_name, clickhouse_client):

    table_name = 'tbl_salesitems'
    # Fetch the primary key column(s) for the table
    query = f"""
    SELECT primary_key
    FROM system.tables
    WHERE name = '{table_name}'
    """
    primary_keys = run_query_in_clickhouse(query, clickhouse_client)

    return primary_keys

def delete_rows_from_clickhouse(primary_keys, table_name, df, clickhouse_client, chunk_size=10000):
    start = pd.Timestamp.now()
    extra={
        'database': clickhouse_client.url,
        }
    if df.empty:
        # print("DataFrame is empty. No data to delete.")
        calc_duration = (pd.Timestamp.now() - start).total_seconds()
        extra['calc_duration']=calc_duration
        message = json.dumps({
            'message': 'DataFrame is empty. No data to delete.',
            'primary_keys': primary_keys,
            'table_name': table_name
            })
        logger.info(message, extra=extra)
        return

    def chunked_delete(key, values):
        for i in range(0, len(values), chunk_size):
            chunk = values[i:i + chunk_size]
            chunk_values = ', '.join(map(repr, chunk))
            delete_query = f"DELETE FROM {table_name} WHERE {key} IN ({chunk_values})"
            run_query_in_clickhouse(delete_query, clickhouse_client)

    if isinstance(primary_keys, list):
        if len(primary_keys) == 1:
            # Case a: Single primary key, use IN syntax with chunking
            key = primary_keys[0]
            values = df[key].tolist()
            chunked_delete(key, values)
        else:
            # Case b: Multiple primary keys, iterate through rows
            for index, row in df.iterrows():
                conditions = " AND ".join([f"{key} = {repr(row[key])}" for key in primary_keys])
                delete_query = f"DELETE FROM {table_name} WHERE {conditions}"
                run_query_in_clickhouse(delete_query, clickhouse_client)

    elif isinstance(primary_keys, str):
        # Case: Single primary key as string, use IN syntax with chunking
        values = df[primary_keys].tolist()
        chunked_delete(primary_keys, values)
    else:
        error = "primary_keys must be either a list or a string"
        extra['error'] = error
        logger.exception(error, extra=extra)
        raise ValueError(error)

    # print(f"Deleted {len(df)} rows from {table_name}")
    calc_duration = (pd.Timestamp.now() - start).total_seconds()
    extra['calc_duration']=calc_duration
    message = json.dumps({
        'message': f'Deleted {len(df)} rows',
        'primary_keys': primary_keys,
        'table_name': table_name
        })
    logger.info(message, extra=extra)

def delete_temporal_data_from_clickhouse(current, temporal_interval, temporal_column, table_name, clickhouse_client):
    start = pd.Timestamp.now()

    end = current + temporal_interval
    delete_query = f"""
    DELETE FROM {table_name}
    WHERE {temporal_column} >= '{current.date()}' AND {temporal_column} < '{end.date()}'
    """
    extra={
        'query':delete_query,
        'database': clickhouse_client.url,
        }
    current.strftime
    run_query_in_clickhouse(delete_query, clickhouse_client)
    # print(f"Deleted {current.date()} from {table_name}")   
    calc_duration = (pd.Timestamp.now() - start).total_seconds()
    extra['calc_duration']=calc_duration
    message = json.dumps({
        'message': f'Deleted {current.date()} from {table_name}.',
        'current': current.isoformat(),
        'temporal_interval': str(temporal_interval),
        'temporal_column': temporal_column,
        'table_name': table_name
        })
    logger.info(message, extra=extra)

def insert_into_clickhouse(table_name, df, clickhouse_client, chunk_size=10000):
    start = pd.Timestamp.now()
    extra={
        'database': clickhouse_client.url,
        }
    # Ensure the DataFrame is not empty
    if df.empty:
        # print("DataFrame is empty. No data to insert.")
        calc_duration = (pd.Timestamp.now() - start).total_seconds()
        extra['calc_duration']=calc_duration
        message = json.dumps({
            'message': 'DataFrame is empty. No data to insert.',
            'table_name': table_name
            })
        logger.info(message, extra=extra)
        return

    # Insert the DataFrame into ClickHouse in chunks
    total_rows = len(df)
    for i in range(0, total_rows, chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        insert_DataFrameinto_clickhouse(table_name, chunk, clickhouse_client)
    # print(f"Inserted all {total_rows} rows into {table_name}")
    calc_duration = (pd.Timestamp.now() - start).total_seconds()
    extra['calc_duration']=calc_duration
    message = json.dumps({
        'message': f'Inserted {total_rows} rows into {table_name}',
        'df.size': df.size,
        'df.dtypes': df.dtypes.astype(str).to_dict(),
        'table_name': table_name
        })
    logger.info(message, extra=extra)


def get_max_from_clickhouse(column, table_name, clickhouse_client):
    query = f"SELECT MAX({column}) FROM {table_name}"
    result = run_query_in_clickhouse(query, clickhouse_client)
    return result

def get_min_from_clickhouse(column, table_name, clickhouse_client):
    query = f"SELECT MIN({column}) FROM {table_name}"
    result = run_query_in_clickhouse(query, clickhouse_client)
    return result

def get_max_date_from_clickhouse(temporal_column, table_name, clickhouse_client):
    result = get_max_from_clickhouse(temporal_column, table_name, clickhouse_client)
    if result == None or result == '\\N':
        return None
    else:
        return pd.to_datetime(result)

def get_min_date_from_clickhouse(temporal_column, table_name, clickhouse_client):
    result = get_min_from_clickhouse(temporal_column, table_name, clickhouse_client)
    if result == None or result == '\\N':
        return None
    else:
        return pd.to_datetime(result)

def get_max_from_mariadb(column, table_name, mariadb_engine):
    query = f"SELECT MAX({column}) FROM {table_name}"
    result = run_query_in_mariadb(query, mariadb_engine)
    return result[0][0]

def get_min_from_mariadb(column, table_name, mariadb_engine):
    query = f"SELECT MIN({column}) FROM {table_name}"
    result = run_query_in_mariadb(query, mariadb_engine)
    return result[0][0]

def get_max_date_from_mariadb(temporal_column, table_name, mariadb_engine):
    result = get_max_from_mariadb(temporal_column, table_name, mariadb_engine)
    if result == None or result == '\\N':
        return None
    else:
        return pd.to_datetime(result)

def get_min_date_from_mariadb(temporal_column, table_name, mariadb_engine):
    result = get_min_from_mariadb(temporal_column, table_name, mariadb_engine)
    if result == None or result == '\\N':
        return None
    else:
        return pd.to_datetime(result)