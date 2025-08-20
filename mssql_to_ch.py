import os
import pyodbc
from clickhouse_driver import Client
import logging
from os import environ
from tqdm import tqdm
import time
from decimal import Decimal
import datetime

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
    """Создает клиент ClickHouse с надежным подключением"""
    try:
        client = Client(
            host=environ.get('CH_HOST', 'clickhouse'),
            port=int(environ.get('CH_PORT', '9000')),
            user=environ.get('CH_USER', 'admin'),
            password=environ.get('CH_PASSWORD', 'admin123'),
            settings={
                'use_numpy': False,
                'max_insert_block_size': 100000,
                'connect_timeout': 30,
                'send_receive_timeout': 600,
                'insert_block_size': 50000,
                'async_insert': 1
            }
        )
        # Проверяем соединение
        client.execute('SELECT 1')
        logger.info("Успешное подключение к ClickHouse")
        return client
    except Exception as e:
        logger.error(f"Ошибка подключения к ClickHouse: {str(e)}")
        raise ConnectionError(f"Не удалось подключиться к ClickHouse: {str(e)}")

def convert_value(value, column_name=None):
    """
    Конвертирует значения для ClickHouse.
    Числовые → float, даты → date, строки → string, None → '' или 0.0
    """
    numeric_columns = {
        'weight', 'sales_quantity', 'sales_amount_rub',
        'avg_cost_price', 'avg_sell_price',
        'sales_amount_with_vat', 'promo_sales_amount_with_vat',
        'writeoff_quantity', 'writeoff_amount_rub',
        'margin_amount_rub', 'loss_quantity', 'loss_amount_rub',
        'sales_tons', 'sales_weight_kg'
    }

    # 1. None → дефолты
    if value is None:
        if column_name == 'sale_date':
            return datetime.date(1970, 1, 1)
        elif column_name in numeric_columns:
            return 0.0
        else:
            return ''  # для строк

    # 2. Decimal
    if isinstance(value, Decimal):
        return float(value)

    # 3. Дата
    if isinstance(value, datetime.datetime):
        return value.date() if column_name == 'sale_date' else value
    if isinstance(value, datetime.date):
        return value

    # 4. Строки
    if isinstance(value, str):
        text = value.strip()
        if text in ['', '-', 'nan', 'NaN', 'null', 'None']:
            return 0.0 if column_name in numeric_columns else ''
        if column_name == 'sale_date':
            try:
                return datetime.datetime.strptime(text, '%Y-%m-%d').date()
            except Exception:
                return datetime.date(1970, 1, 1)
        if column_name in numeric_columns:
            try:
                return float(text.replace(',', '.'))
            except Exception:
                return 0.0
        return text

    # 5. Булевое
    if isinstance(value, bool):
        return int(value)

    # 6. Int / float
    if isinstance(value, (int, float)):
        return value

    # 7. Fallback
    try:
        return str(value)
    except Exception:
        return ''


def transfer_table(full_table_name, target_table=None, batch_size=50000):
    """Оптимизированный перенос данных из MS SQL в ClickHouse"""
    start_time = time.time()
    
    try:
        schema, table_name = full_table_name.split('.') if '.' in full_table_name else ('dbo', full_table_name)
        target_table = target_table or table_name
        
        logger.info(f"Начало переноса из {schema}.{table_name} в {target_table}")

        # Подключаемся к базам данных
        with get_mssql_connection() as mssql_conn, get_clickhouse_client() as ch_client:
            mssql_cursor = mssql_conn.cursor()
            
            # 1. Сначала создаем таблицу в ClickHouse (если не существует)
            logger.info(f"Создание/проверка таблицы {target_table} в ClickHouse")
            ch_client.execute(f"""
                CREATE TABLE IF NOT EXISTS {target_table} (
                    id UInt64,
                    retail_chain String,
                    sale_year UInt16,
                    sale_month UInt8,
                    sale_date Date,
                    branch String,
                    region String,
                    city String,
                    address String,
                    store_format String,
                    store_name String,
                    product_name String,
                    brand String,
                    flavor String,
                    weight Float64,
                    product_type String,
                    package_type String,
                    product_level_1 String,
                    product_level_2 String,
                    product_level_3 String,
                    product_level_4 String,
                    product_family_code String,
                    product_family_name String,
                    product_article String,
                    product_code String,
                    barcode String,
                    factory_code String,
                    factory_name String,
                    material String,
                    vendor String,
                    supplier String,
                    warehouse_supplier String,
                    sales_quantity Float64,
                    sales_amount_rub Float64,
                    avg_cost_price Float64,
                    avg_sell_price Float64,
                    sales_amount_with_vat Float64,
                    promo_sales_amount_with_vat Float64,
                    writeoff_quantity Float64,
                    writeoff_amount_rub Float64,
                    margin_amount_rub Float64,
                    loss_quantity Float64,
                    loss_amount_rub Float64,
                    sales_tons Float64,
                    sales_weight_kg Float64
                ) ENGINE = MergeTree()
                ORDER BY (sale_date, id)
                SETTINGS index_granularity = 8192
            """)
            
            # 2. Теперь получаем информацию о колонках для правильного преобразования типов
            logger.info("Получение информации о колонках ClickHouse")
            columns_info = ch_client.execute(f"DESCRIBE TABLE {target_table}")
            columns_map = {col[0]: col[1] for col in columns_info}
            logger.debug(f"Колонки ClickHouse: {list(columns_map.keys())}")
            
            # 3. Получаем максимальный ID из ClickHouse
            try:
                max_id_ch = ch_client.execute(f"SELECT max(id) FROM {target_table}")[0][0] or 0
                logger.info(f"Текущий максимальный ID в ClickHouse: {max_id_ch}")
            except Exception as e:
                logger.warning(f"Не удалось получить max ID (возможно таблица пустая): {str(e)}")
                max_id_ch = 0

            # 4. Получаем общее количество новых строк из MS SQL
            count_query = f"SELECT COUNT(*) FROM [{schema}].[{table_name}] WHERE id > {max_id_ch}"
            logger.debug(f"Выполняем запрос: {count_query}")
            mssql_cursor.execute(count_query)
            total_rows = mssql_cursor.fetchone()[0]
            
            if total_rows == 0:
                logger.info("Нет новых данных для переноса")
                return
                
            logger.info(f"Найдено {total_rows:,} новых строк для переноса")

            # 5. Получаем информацию о колонках из MS SQL
            mssql_cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'
                ORDER BY ORDINAL_POSITION
            """)
            mssql_columns = {row[0]: row[1] for row in mssql_cursor.fetchall()}
            logger.debug(f"Колонки MS SQL: {list(mssql_columns.keys())}")

            # 6. Переносим данные пакетами
            transferred_rows = 0
            last_id = max_id_ch
            error_count = 0
            error_log_path = f'error_rows_{target_table}_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
            
            with open(error_log_path, 'w', encoding='utf-8') as error_log, \
                 tqdm(total=total_rows, unit='rows', desc=f"Перенос {table_name}") as pbar:
                
                error_log.write(f"Лог ошибок переноса таблицы {target_table}\n")
                error_log.write(f"Время начала: {datetime.datetime.now()}\n")
                error_log.write("="*50 + "\n")
                
                batch_count = 0
                while True:
                    batch_count += 1
                    logger.debug(f"Обработка батча #{batch_count}, last_id={last_id}")
                    
                    # Эффективный запрос с использованием ключа (id)
                    query = f"""
                    SELECT TOP {batch_size} *
                    FROM [{schema}].[{table_name}]
                    WHERE id > {last_id}
                    ORDER BY id
                    """
                    
                    mssql_cursor.execute(query)
                    rows = mssql_cursor.fetchall()
                    
                    if not rows:
                        logger.debug("Больше данных нет для переноса")
                        break
                    
                    # Получаем имена колонок из MS SQL
                    columns = [column[0] for column in mssql_cursor.description]
                    logger.debug(f"Колонки в результате: {columns}")
                    
                    # Преобразуем данные для ClickHouse
                    data = []
                    batch_errors = 0
                    
                    for row_idx, row in enumerate(rows):
                        try:
                            processed_row = []
                            for i, value in enumerate(row):
                                column_name = columns[i] if i < len(columns) else f'column_{i}'
                                processed_value = convert_value(value, column_name)
                                processed_row.append(processed_value)
                            
                            data.append(processed_row)
                            last_id = row[0]  # предполагаем, что id - первый столбец
                            
                        except Exception as e:
                            error_count += 1
                            batch_errors += 1
                            error_msg = f"Ошибка обработки строки {row[0] if row else 'unknown'}: {str(e)}"
                            error_log.write(error_msg + "\n")
                            logger.debug(error_msg)
                            continue
                    
                    if batch_errors > 0:
                        logger.warning(f"В батче #{batch_count} обнаружено {batch_errors} ошибок обработки")
                    
                    # Вставка данных в ClickHouse
                    if data:
                        try:
                            ch_client.execute(
                                f"INSERT INTO {target_table} VALUES",
                                data,
                                types_check=True
                            )
                            transferred_rows += len(data)
                            pbar.update(len(data))
                            logger.debug(f"Успешно вставлен батч #{batch_count} из {len(data)} строк")
                            
                        except Exception as e:
                            logger.error(f"Ошибка вставки батча #{batch_count}: {str(e)}")
                            
                            # Пробуем вставить по одной строке
                            single_success = 0
                            for single_row in data:
                                try:
                                    ch_client.execute(
                                        f"INSERT INTO {target_table} VALUES",
                                        [single_row],
                                        types_check=True
                                    )
                                    transferred_rows += 1
                                    single_success += 1
                                    pbar.update(1)
                                except Exception as single_e:
                                    error_count += 1
                                    error_msg = f"Ошибка вставки строки {single_row[0]}: {str(single_e)}"
                                    error_log.write(error_msg + "\n")
                                    logger.debug(error_msg)
                                    continue
                            
                            if single_success > 0:
                                logger.info(f"Удалось вставить {single_success} строк по одной из проблемного батча")
                    
                    # Небольшая пауза между батчами чтобы не перегружать системы
                    if batch_count % 10 == 0:
                        time.sleep(0.1)
            
            # 7. Финализация
            total_time = time.time() - start_time
            logger.info(
                f"Перенос завершен. Перенесено {transferred_rows:,} строк "
                f"за {total_time:.2f} сек ({transferred_rows/total_time:,.0f} строк/сек)"
            )
            
            if error_count > 0:
                logger.warning(f"Обнаружено {error_count} ошибок. Подробности в {error_log_path}")
            else:
                # Удаляем пустой лог ошибок
                if os.path.exists(error_log_path):
                    os.remove(error_log_path)
            
            # Проверяем итоговое количество строк
            try:
                final_count = ch_client.execute(f"SELECT count() FROM {target_table}")[0][0]
                logger.info(f"Итоговое количество строк в {target_table}: {final_count:,}")
            except Exception as e:
                logger.warning(f"Не удалось проверить итоговое количество строк: {str(e)}")
            
    except Exception as e:
        logger.error(f"Критическая ошибка при переносе таблицы {target_table}: {str(e)}", exc_info=True)
        raise
    
if __name__ == "__main__":
    try:
        # Настройки из переменных окружения
        os.environ.setdefault('MSSQL_SERVER', 'host.docker.internal')
        os.environ.setdefault('MSSQL_PORT', '1433')
        os.environ.setdefault('MSSQL_USER', 'superset_user')
        os.environ.setdefault('MSSQL_PASSWORD', '123')
        
        os.environ.setdefault('CH_HOST', 'clickhouse')
        os.environ.setdefault('CH_PORT', '9000')
        os.environ.setdefault('CH_USER', 'admin')
        os.environ.setdefault('CH_PASSWORD', 'admin123')
        
        transfer_table(
            full_table_name='bi.ALL_DATA_COMPETITORS_MATERIALIZED',
            batch_size=50000  # Увеличенный размер блока
        )
    except Exception as e:
        logger.error(f"Фатальная ошибка: {str(e)}", exc_info=True)
        exit(1)