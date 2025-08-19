import pyodbc
from clickhouse_driver import Client
import time

def transfer_data():
    print("Starting data transfer from MS SQL to ClickHouse...")
    
    # Подключение к MS SQL
    mssql_conn = None
    max_retries = 3
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            mssql_conn = pyodbc.connect(
                'DRIVER={ODBC Driver 17 for SQL Server};'
                'SERVER=host.docker.internal,1433;'
                'DATABASE=Stage;'
                'UID=superset_user;'
                'PWD=123;'
                'Encrypt=no;'
                'TrustServerCertificate=yes;'
            )
            break
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise Exception("Failed to connect to MS SQL Server")
    
    # Подключение к ClickHouse
    ch_client = Client(
        host='clickhouse',
        port=9000,
        user='admin',
        password='123',
        settings={'use_numpy': False}
    )
    
    # Пример переноса данных для таблицы 'your_table'
    table_name = 'your_table'
    target_table = 'mssql_' + table_name
    
    print(f"Transferring data from {table_name} to {target_table}...")
    
    # Получаем данные из MS SQL
    cursor = mssql_conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    
    # Получаем описание столбцов
    columns = [column[0] for column in cursor.description]
    column_types = []
    
    for column in cursor.description:
        sql_type = column[1]
        if sql_type == str:
            ch_type = 'String'
        elif sql_type == int:
            ch_type = 'Int32'
        elif sql_type == float:
            ch_type = 'Float64'
        elif 'datetime' in str(sql_type).lower():
            ch_type = 'DateTime'
        else:
            ch_type = 'String'
        column_types.append(ch_type)
    
    # Создаем таблицу в ClickHouse
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        {', '.join([f'{col} {typ}' for col, typ in zip(columns, column_types)])}
    ) ENGINE = MergeTree()
    ORDER BY tuple()
    """
    
    ch_client.execute(create_table_sql)
    print(f"Table {target_table} created in ClickHouse")
    
    # Вставляем данные пакетами
    batch_size = 10000
    total_rows = len(rows)
    print(f"Total rows to transfer: {total_rows}")
    
    for i in range(0, total_rows, batch_size):
        batch = rows[i:i + batch_size]
        ch_client.execute(f'INSERT INTO {target_table} VALUES', batch)
        print(f"Inserted {min(i + batch_size, total_rows)}/{total_rows} rows")
    
    print("Data transfer completed successfully!")

if __name__ == "__main__":
    transfer_data()