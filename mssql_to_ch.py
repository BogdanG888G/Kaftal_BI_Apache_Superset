import os
import pyodbc
from clickhouse_driver import Client
import pandas as pd
import logging
from os import environ
from tqdm import tqdm  # Для прогресс-бара
import time
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_transfer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_mssql_connection():
    """Создает подключение к MS SQL Server"""
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={environ['MSSQL_SERVER']},{environ['MSSQL_PORT']};"
            f"DATABASE=Stage;"
            f"UID={environ['MSSQL_USER']};"
            f"PWD={environ['MSSQL_PASSWORD']};"
            "Encrypt=no;TrustServerCertificate=yes;"
        )
        logger.info(f"Подключаемся к MS SQL: {conn_str.replace(environ['MSSQL_PASSWORD'], '***')}")
        return pyodbc.connect(conn_str)
    except Exception as e:
        logger.error(f"Ошибка подключения к MS SQL: {str(e)}")
        raise

def get_clickhouse_client():
    """Создает клиент ClickHouse"""
    try:
        client = Client(
            host=environ.get('CH_HOST', 'clickhouse'),
            port=int(environ.get('CH_PORT', '9000')),
            user=environ.get('CH_USER', 'admin'),
            password=environ.get('CH_PASSWORD', '123'),
            settings={
                'use_numpy': False,  # Включаем numpy для ускорения
                'allow_experimental_object_type': 1,
                'async_insert': 0,  # Асинхронная вставка
                'wait_for_async_insert': 0,  # Не ждем завершения асинхронной вставки
                'max_insert_block_size': 100_000  # Увеличиваем размер блока вставки
            }
        )
        logger.info("Успешное подключение к ClickHouse")
        return client
    except Exception as e:
        logger.error(f"Ошибка подключения к ClickHouse: {str(e)}")
        raise

def check_table_exists(cursor, schema, table_name):
    """Проверяет существование таблицы"""
    try:
        cursor.execute(f"""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{table_name}'
        """)
        return cursor.fetchone()[0] > 0
    except Exception as e:
        logger.error(f"Ошибка при проверке таблицы: {str(e)}")
        return False

def get_total_rows(cursor, schema, table_name):
    """Получает общее количество строк в таблице"""
    try:
        cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table_name}]")
        return cursor.fetchone()[0]
    except Exception as e:
        logger.error(f"Ошибка при получении количества строк: {str(e)}")
        return 0

def transfer_table(full_table_name, target_table=None, batch_size=100000):
    """Переносит данные из MS SQL в ClickHouse (оптимизированная версия)"""
    start_time = time.time()
    try:
        # Разбираем имя таблицы
        schema, table_name = full_table_name.split('.') if '.' in full_table_name else ('dbo', full_table_name)
        target_table = target_table or table_name
        
        logger.info(f"Начало переноса из {schema}.{table_name} в {target_table}")

        # Подключаемся к MS SQL
        with get_mssql_connection() as mssql_conn:
            cursor = mssql_conn.cursor()
            
            # Получаем общее количество строк
            total_rows = get_total_rows(cursor, schema, table_name)
            logger.info(f"Всего строк для переноса: {total_rows:,}")
            
            # Получаем структуру таблицы
            cursor.execute(f"SELECT TOP 0 * FROM [{schema}].[{table_name}]")
            columns = [column[0] for column in cursor.description]
            
            # Подключаемся к ClickHouse с оптимизированными настройками
            ch_client = get_clickhouse_client()
            
            # Переносим данные пакетами с использованием server-side cursor
            offset = 0
            transferred_rows = 0
            
            # Настраиваем прогресс-бар
            with tqdm(total=total_rows, unit='rows', desc=f"Перенос {table_name}") as pbar:
                while offset < total_rows:
                    batch_start_time = time.time()
                    
                    # Используем серверный курсор для эффективной выборки
                    query = f"""
                    SELECT * FROM (
                        SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as row_num 
                        FROM [{schema}].[{table_name}]
                    ) t WHERE row_num BETWEEN {offset + 1} AND {offset + batch_size}
                    """
                    
                    # Читаем данные без pandas (для экономии памяти)
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    
                    if not rows:
                        break
                    
                    # Вставляем данные напрямую в ClickHouse
                    try:
                        ch_client.execute(
                            f"INSERT INTO {target_table} VALUES",
                            rows,
                            types_check=False,  # Отключаем проверку типов для скорости
                            columnar=True     # Явно отключаем columnar режим
                        )
                    except Exception as e:
                        logger.error(f"Ошибка при вставке блока: {str(e)}")
                        # Альтернативный метод вставки при ошибке
                        values = ','.join([str(tuple(row)) for row in rows])
                        ch_client.execute(f"INSERT INTO {target_table} VALUES {values}")
                    
                    rows_inserted = len(rows)
                    transferred_rows += rows_inserted
                    offset += rows_inserted
                    
                    # Обновляем прогресс-бар
                    pbar.update(rows_inserted)
                    
                    # Логируем статистику по батчу
                    batch_time = time.time() - batch_start_time
                    rows_per_sec = rows_inserted / batch_time if batch_time > 0 else 0
                    
                    logger.info(
                        f"Блок {offset//batch_size}: {rows_inserted:,} строк за {batch_time:.2f} сек "
                        f"({rows_per_sec:,.0f} строк/сек). Всего: {transferred_rows:,}/{total_rows:,}"
                    )
            
            total_time = time.time() - start_time
            logger.info(
                f"Перенос завершен. Всего строк: {transferred_rows:,} "
                f"за {total_time:.2f} сек ({transferred_rows/total_time:,.0f} строк/сек)"
            )
    
    except Exception as e:
        logger.error(f"Ошибка при переносе: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    try:
        # Настройки из переменных окружения
        table_to_transfer = os.getenv('TABLE_TO_TRANSFER', 'bi.ALL_DATA_COMPETITORS_MATERIALIZED')
        target_table = os.getenv('TARGET_TABLE', None)
        batch_size = int(os.getenv('BATCH_SIZE', '100000'))
        
        logger.info(f"Запуск переноса таблицы: {table_to_transfer}")
        logger.info(f"Параметры: batch_size={batch_size}, target_table={target_table}")
        
        transfer_table(
            full_table_name=table_to_transfer,
            target_table=target_table,
            batch_size=batch_size
        )
    except Exception as e:
        logger.error(f"Фатальная ошибка: {str(e)}", exc_info=True)
        exit(1)