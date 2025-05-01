from sqlalchemy import create_engine
import clickhouse_connect
gong_cha_redcat_db_clickhouse_conn_params = {
    'host': '172.105.163.229',
    'port': '8123',
    'username': 'eddy',
    'password': 'jdd6HBrv',
    'database': 'gong_cha_redcat_db',
}
gong_cha_redcat_db_clickhouse_client = clickhouse_connect.get_client(**gong_cha_redcat_db_clickhouse_conn_params)

# Define a function that creates the connection URL and engine
def create_engine_from_config(db_config):
    # Determine the appropriate connection prefix based on the database type
    if db_config['type'] == 'postgres':
        connection_prefix = 'postgresql+psycopg2'
    elif db_config['type'] == 'mysql':
        connection_prefix = 'mysql+pymysql'
    else:
        raise ValueError("Unsupported database type. Use 'postgres' or 'mysql'.")

    # Create the connection URL using the dictionary
    connection_url = f"{connection_prefix}://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    
    # Create the SQLAlchemy engine
    engine = create_engine(connection_url)
    
    return connection_url, engine

# Define a function that creates the connection URL and engine
def create_mysql_engine_from_config(db_config):
    # Determine the appropriate connection prefix based on the database type

    connection_prefix = 'mysql+pymysql'

    # Create the connection URL using the dictionary
    connection_url = f"{connection_prefix}://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    
    # Create the SQLAlchemy engine
    engine = create_engine(connection_url)
    
    return connection_url, engine

def create_postgres_engine_from_config(db_config):
    # Determine the appropriate connection prefix based on the database type
    connection_prefix = 'postgresql+psycopg2'
    # Create the connection URL using the dictionary
    connection_url = f"{connection_prefix}://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    # Create the SQLAlchemy engine
    engine = create_engine(connection_url)
    
    return connection_url, engine

pxprodgongchaau_mariadb_redcat_config = {
    'user': 'GongChaAUData',
    'password': 'Pamxf&7HmPCh9D',
    'host': 'rdb-mariadb-gongcha-prod-read-01.cp9r0gu11n6n.ap-southeast-2.rds.amazonaws.com',
    'port': 3306,
    'database': 'pxprodgongchaau'
}
pxprodgongchaau_mariadb_redcat_conn, pxprodgongchaau_mariadb_redcat_engine = create_mysql_engine_from_config(pxprodgongchaau_mariadb_redcat_config)

gong_cha_redcat_db_mysql_gca_config = {
    'user': 'gong-cha',
    'password': 'HelloGongCha2012',
    'host': '34.116.84.145',
    'port': 3306,
    'database': 'gong_cha_redcat_db'
}
gong_cha_redcat_db_mysql_gca_conn, gong_cha_redcat_db_mysql_gca_engine = create_mysql_engine_from_config(gong_cha_redcat_db_mysql_gca_config)

gong_cha_aupos_db_mysql_gca_config = {
    'user': 'gong-cha',
    'password': 'HelloGongCha2012',
    'host': '34.116.84.145',
    'port': 3306,
    'database': 'gong_cha_db'
}
gong_cha_aupos_db_mysql_gca_conn, gong_cha_aupos_db_mysql_gca_engine = create_mysql_engine_from_config(gong_cha_aupos_db_mysql_gca_config)

time_series_forecast_timescale_gca_config = {
    'username': 'eddy',
    'password': 'jdd6HBrv',
    'host': '172.105.162.160',
    'port': '5432',
    'database': 'time_series_forecast'
}
time_series_forecast_timescale_gca_conn, time_series_forecast_timescale_gca_engine = create_postgres_engine_from_config(time_series_forecast_timescale_gca_config)

gong_cha_redcat_db_timescale_gca_config = {
    'username': 'eddy',
    'password': 'jdd6HBrv',
    'host': '172.105.162.160',
    'port': '5432',
    'database': 'gong_cha_redcat_db'
}
gong_cha_redcat_db_timescale_gca_conn, gong_cha_redcat_db_timescale_gca_engine = create_postgres_engine_from_config(gong_cha_redcat_db_timescale_gca_config)

telegram_db_mysql_gca_config = {
    'user': 'gong-cha',
    'password': 'HelloGongCha2012',
    'host': '34.116.84.145',
    'port': 3306,
    'database': 'telegram_db'
}
telegram_db_mysql_gca_conn, telegram_d_mysql_gca_engine = create_mysql_engine_from_config(telegram_db_mysql_gca_config)

telegram_db_timescale_gca_config = {
    'username': 'eddy',
    'password': 'jdd6HBrv',
    'host': '172.105.162.160',
    'port': '5432',
    'database': 'telegram_db'
}
telegram_db_timescale_gca_conn, telegram_db_timescale_gca_engine = create_postgres_engine_from_config(telegram_db_timescale_gca_config)

gong_cha_redcat_db_pgduckdb_config = {
    'username': 'postgres',
    'password': 'duckdb',
    'host': '172.105.170.194',
    'port': '5432',
    'database': 'gong_cha_redcat_db'
}
gong_cha_redcat_db_pgduckdb_conn, gong_cha_redcat_db_pgduckdb_engine = create_postgres_engine_from_config(gong_cha_redcat_db_pgduckdb_config)
