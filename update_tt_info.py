import requests
import logging
import time
import urllib.parse
from typing import Dict, Optional, Tuple, List
import re
import time
import logging
from typing import Dict, Any, List
import threading
import os
import pyodbc
import numpy as np
from tqdm import tqdm
from datetime import date

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Глобальная переменная для кэша подключений
_db_connections = {}
_db_lock = threading.Lock()

def get_db_connection():
    """Получение подключения к базе данных"""
    global _db_connections, _db_lock
    
    thread_id = threading.get_ident()
    
    with _db_lock:
        if thread_id in _db_connections:
            try:
                # Проверяем, что подключение еще живо
                _db_connections[thread_id].cursor().execute("SELECT 1")
                return _db_connections[thread_id]
            except:
                # Если подключение разорвано, удаляем его из кэша
                del _db_connections[thread_id]
        
        # Создаем новое подключение
        server = os.environ.get('MSSQL_SERVER', 'host.docker.internal')
        database = os.environ.get('MSSQL_DATABASE', 'Stage')
        username = os.environ.get('MSSQL_USER', 'superset_user')
        password = os.environ.get('MSSQL_PASSWORD', '123')
        port = os.environ.get('MSSQL_PORT', '1433')
        
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;"
            "Connection Timeout=30;"
        )
        
        logger.info(f"Создаем новое подключение к SQL Server: {server},{port}")
        conn = pyodbc.connect(conn_str)
        _db_connections[thread_id] = conn
        
        return conn

class YandexGeoProcessor:
    def __init__(self, api_keys: List[str] = None):
        self.api_keys = api_keys or []
        self.current_key_index = 0
        self.geocoder_url = "https://geocode-maps.yandex.ru/1.x/"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        })
        
        # Словари для определения федеральных округов и субъектов
        self.federal_districts = {
            'Центральный федеральный округ': ['Москва', 'Московская область', 'Белгородская область', 'Брянская область', 
                                              'Владимирская область', 'Воронежская область', 'Ивановская область', 
                                              'Калужская область', 'Костромская область', 'Курская область', 'Липецкая область',
                                              'Орловская область', 'Рязанская область', 'Смоленская область', 'Тамбовская область',
                                              'Тверская область', 'Тульская область', 'Ярославская область'],
            'Северо-Западный федеральный округ': ['Санкт-Петербург', 'Ленинградская область', 'Архангельская область', 
                                                  'Вологодская область', 'Калининградская область', 'Республика Карелия',
                                                  'Республика Коми', 'Мурманская область', 'Ненецкий автономный округ',
                                                  'Новгородская область', 'Псковская область'],
            'Южный федеральный округ': ['Республика Адыгея', 'Астраханская область', 'Волгоградская область', 'Республика Калмыкия',
                                        'Краснодарский край', 'Ростовская область', 'Республика Крым', 'Севастополь'],
            'Северо-Кавказский федеральный округ': ['Республика Дагестан', 'Республика Ингушетия', 'Кабардино-Балкарская Республика',
                                                    'Карачаево-Черкесская Республика', 'Республика Северная Осетия — Алания',
                                                    'Чеченская Республика', 'Ставропольский край'],
            'Приволжский федеральный округ': ['Республика Башкортостан', 'Кировская область', 'Республика Марий Эл', 
                                              'Республика Мордовия', 'Нижегородская область', 'Оренбургская область', 
                                              'Пензенская область', 'Пермский край', 'Самарская область', 'Саратовская область',
                                              'Республика Татарстан', 'Удмуртская Республика', 'Ульяновская область', 
                                              'Чувашская Республика'],
            'Уральский федеральный округ': ['Курганская область', 'Свердловская область', 'Тюменская область', 
                                            'Челябинская область', 'Ханты-Мансийский автономный округ — Югра', 
                                            'Ямало-Ненецкий автономный округ'],
            'Сибирский федеральный округ': ['Республика Алтай', 'Алтайский край', 'Иркутская область', 'Кемеровская область',
                                            'Красноярский край', 'Новосибирская область', 'Омская область', 'Томская область',
                                            'Республика Тыва', 'Республика Хакасия'],
            'Дальневосточный федеральный округ': ['Амурская область', 'Еврейская автономная область', 'Камчатский край',
                                                  'Магаданская область', 'Приморский край', 'Республика Саха (Якутия)',
                                                  'Сахалинская область', 'Хабаровский край', 'Чукотский автономный округ']
        }
        
        # Создаем обратный словарь для быстрого поиска округа по субъекту
        self.subject_to_district = {}
        for district, subjects in self.federal_districts.items():
            for subject in subjects:
                self.subject_to_district[subject.lower()] = district

        # Расширенный словарь для сопоставления регионов
        self.regions_mapping = {}
        
        # Словарь для сопоставления городов и регионов
        self.city_to_region = {}
        
        # Обновленные диапазоны площадей для различных сетей и форматов
        self.area_ranges = {
            'магнит': {
                'пр': (250, 350),        # Продуктовый магазин
                'бф': (800, 1200),       # Бизнес-формат
                'мд': (300, 500),        # Магазин у дома
                'мк': (200, 300),        # Магнит Косметик
                'default': (250, 350)
            },
            'ашан': {
                'ашан': (8000, 12000),    # Гипермаркет
                'ашан сити': (800, 1200), # Супермаркет
                'дарк стор': (500, 1500), # Темный склад
                'наша радуга': (800, 2500), # Супермаркет
                'default': (1000, 2000)
            },
            'пятерочка': {
                'default': (300, 450)
            },
            'пятёрочка': {
                'default': (300, 450)
            },
            'перекресток': {
                'default': (750, 1200)
            },
            'перекрёсток': {
                'default': (750, 1200)
            },
            'дикси': {
                'дикси': (300, 500),
                'default': (300, 500)
            },
            'окей': {
                'default': (5000, 10000)
            },
            'x5 united': {
                'default': (750, 1200)
            },
            'пятъница': {
                'default': (250, 350)
            },
            'чижик': {
                'default': (250, 350)
            },
            'перекрёсток-джем': {
                'default': (250, 350)
            },
            'default': {
                'гипермаркет': (8000, 12000),
                'супермаркет': (700, 1000),
                'магазин у дома': (200, 300),
                'торговый центр': (5000, 20000),
                'складской клуб': (3000, 5000),
                'default': (200, 400)
            }
        }

        # Словарь для определения store_type на основе сети и формата
        self.store_type_mapping = {
            'магнит': {
                'пр': 'магазин у дома'.capitalize(),
                'бф': 'супермаркет'.capitalize(),
                'мд': 'магазин у дома'.capitalize(),
                'мк': 'косметический'.capitalize(),
                'default': 'магазин у дома'.capitalize()    
            },
            'ашан': {
                'ашан': 'гипермаркет'.capitalize(),
                'ашан сити': 'супермаркет'.capitalize(),
                'дарк стор': 'тёмный склад'.capitalize(),
                'наша радуга': 'супермаркет'.capitalize(),
                'default': 'гипермаркет'.capitalize()
            },
            'пятерочка': {'default': 'магазин у дома'.capitalize()},
            'пятёрочка': {'default': 'магазин у дома'.capitalize()},
            'перекресток': {'default': 'супермаркет'.capitalize()},
            'перекрёсток': {'default': 'супермаркет'.capitalize()},
            'дикси': {'дикси': 'магазин у дома'.capitalize(), 'default': 'магазин у дома'.capitalize()},
            'окей': {'default': 'гипермаркет'.capitalize()},
            'x5 united': {'default': 'магазин у дома'.capitalize()},
            'пятъница': {'default': 'магазин у дома'.capitalize()},
            'чижик': {'default': 'дискаунтер'.capitalize()},
            'перекрёсток-джем': {'default': 'магазин у дома'.capitalize()},
            'default': {
                'гипермаркет': 'гипермаркет'.capitalize(),
                'супермаркет': 'супермаркет'.capitalize(),
                'магазин у дома': 'магазин у дома'.capitalize(),
                'торговый центр': 'торговый центр'.capitalize(),
                'складской клуб': 'складской'.capitalize(),
                'default': 'магазин у дома'.capitalize()
            }
        }

    def get_current_api_key(self):
        """Получение текущего API ключа"""
        if not self.api_keys:
            return None
        return self.api_keys[self.current_key_index]

    def switch_to_next_key(self):
        """Переключение на следующий API ключ с проверкой на исчерпанность"""
        if not self.api_keys:
            return False
        
        original_index = self.current_key_index
        attempts = 0
        
        while attempts < len(self.api_keys):
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            current_key = self.api_keys[self.current_key_index]
            
            # Проверяем, не исчерпан ли ключ (здесь можно добавить дополнительную логику)
            if not self.is_key_exhausted(current_key):
                logger.info(f"Переключились на ключ: {current_key[:8]}...{current_key[-4:]}")
                return True
            
            attempts += 1
    
    
    def is_key_exhausted(self, key: str) -> bool:
        """Проверяет, исчерпан ли ключ (заглушка, можно реализовать логику проверки)"""
        # Здесь можно добавить логику проверки, был ли ключ уже использован сегодня
        # и исчерпал ли свой лимит. Пока возвращаем False для простоты.
        return False
        
        # Если все ключи исчерпаны
        logger.error("Все доступные ключи исчерпаны!")
        self.current_key_index = original_index  # Возвращаем оригинальный индекс
        return False

    def get_sales_data(self, retail_chain: str, address: str, sale_date: date) -> Dict[str, Any]:
        """Получение данных о продажах из исходной таблицы за конкретную дату"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            sql = """
            SELECT 
                SUM(sales_quantity) as total_quantity,
                SUM(sales_amount_rub) as total_amount,
                AVG(avg_sell_price) as avg_sell,
                AVG(avg_cost_price) as avg_cost
            FROM [Stage].[bi].[ALL_DATA_COMPETITORS_MATERIALIZED]
            WHERE retail_chain = ? AND address = ? AND sale_date = ?
            GROUP BY retail_chain, address, sale_date
            """
            cursor.execute(sql, retail_chain, address, sale_date)
            row = cursor.fetchone()
            
            if row:
                return {
                    'sales_quantity': row.total_quantity or 0,
                    'sales_amount_rub': row.total_amount or 0.0,
                    'avg_sell_price': row.avg_sell or 0.0,
                    'avg_cost_price': row.avg_cost or 0.0
                }
            else:
                return {'sales_quantity': 0, 'sales_amount_rub': 0.0, 'avg_sell_price': 0.0, 'avg_cost_price': 0.0}
        
        except Exception as e:
            logger.error(f"Ошибка при получении данных о продажах: {e}")
            return {'sales_quantity': 0, 'sales_amount_rub': 0.0, 'avg_sell_price': 0.0, 'avg_cost_price': 0.0}


    def get_data_from_source_table(self) -> List[Dict]:
        """Получение новых записей с sale_date"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            sql = """
            SELECT DISTINCT 
                adc.sale_date, 
                adc.retail_chain, 
                adc.store_format, 
                adc.address
            FROM [Stage].[bi].[ALL_DATA_COMPETITORS_MATERIALIZED] adc
            LEFT JOIN [Stage].[bi].[STORE_CHARACTERISTICS] sc 
                ON sc.retail_chain = adc.retail_chain 
                AND sc.address = adc.address
                AND sc.sale_date = adc.sale_date
            WHERE adc.retail_chain IS NOT NULL 
                AND adc.address IS NOT NULL
                AND sc.retail_chain IS NULL
            OPTION (MAXDOP 1)
            """
            cursor.execute(sql)
            rows = cursor.fetchall()
            return [{'sale_date': row.sale_date, 'retail_chain': row.retail_chain,
                    'store_format': row.store_format, 'address': row.address} for row in rows]
        except Exception as e:
            logger.error(f"Ошибка при получении данных из таблицы: {e}")
            return []

    def get_store_type(self, network: str, format_type: str) -> str:
        """Определение типа магазина на основе сети и формата"""
        network_lower = network.lower()
        format_lower = format_type.lower() if format_type else 'default'
        
        if network_lower in self.store_type_mapping:
            network_types = self.store_type_mapping[network_lower]
            if format_lower in network_types:
                return network_types[format_lower]
            return network_types.get('default', 'магазин у дома'.capitalize())
        
        return self.store_type_mapping['default'].get(format_lower, 
                    self.store_type_mapping['default']['default'])

    def get_area_from_range(self, network: str, format_type: str) -> float:
        """Получение площади на основе диапазона для сети и формата"""
        network_lower = network.lower()
        format_lower = format_type.lower() if format_type else 'default'
        
        # Ищем сеть в словаре
        if network_lower in self.area_ranges:
            network_ranges = self.area_ranges[network_lower]
            
            # Ищем конкретный формат
            if format_lower in network_ranges:
                area_range = network_ranges[format_lower]
            else:
                # Используем значение по умолчанию для сети
                area_range = network_ranges.get('default', (200, 400))
        else:
            # Сеть не найдена, используем общие диапазоны по формату
            default_ranges = self.area_ranges['default']
            if format_lower in default_ranges:
                area_range = default_ranges[format_lower]
            else:
                area_range = default_ranges['default']
        
        # Генерируем случайное значение в диапазоне
        return int(np.random.uniform(area_range[0], area_range[1]))


    def save_to_database(self, data: Dict[str, Any]) -> bool:
        """Сохраняет данные о торговой точке в базу данных с учетом sale_date"""
        retail_chain = data.get('network', '')
        store_format = data.get('format', '')
        address = data.get('original_address', data.get('address', ''))
        sale_date = data.get('sale_date') or date.today()

        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            check_sql = """
            SELECT COUNT(*) FROM [Stage].[bi].[STORE_CHARACTERISTICS] 
            WHERE retail_chain = ? AND address = ? AND sale_date = ?
            """
            cursor.execute(check_sql, retail_chain, address, sale_date)
            count = cursor.fetchone()[0]
            if count > 0:
                logger.info(f"Запись уже существует: {retail_chain} - {address} - {sale_date}")
                return False

            city = data.get('city', 'Неизвестно')
            federal_district = data.get('federal_district', 'Неизвестно')
            federal_subject = data.get('federal_subject', 'Неизвестно')
            lat = data.get('coordinates', {}).get('lat', 0)
            lon = data.get('coordinates', {}).get('lon', 0)
            area_m2 = data.get('area') or self.get_area_from_range(retail_chain, store_format)
            has_alcohol_department = 1 if data.get('has_alcohol', False) else 1
            has_snacks = 1 if data.get('has_grocery', False) else 1
            store_type = self.get_store_type(retail_chain, store_format)

            # Получаем агрегированные продажи по дате
            sales_data = self.get_sales_data(retail_chain, address, sale_date)

            sql = """
            INSERT INTO [Stage].[bi].[STORE_CHARACTERISTICS] 
            (retail_chain, store_format, store_type, address, sale_date, city, 
            federal_district, federal_subject,
            sales_quantity, sales_amount_rub, avg_sell_price, avg_cost_price, 
            lat, lon, area_m2, has_alcohol_department, has_snacks, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
            """
            cursor.execute(sql, 
                        retail_chain, store_format, store_type, address, sale_date,
                        city, federal_district, federal_subject,
                        sales_data['sales_quantity'], sales_data['sales_amount_rub'],
                        sales_data['avg_sell_price'], sales_data['avg_cost_price'],
                        lat, lon, area_m2, has_alcohol_department, has_snacks)
            conn.commit()
            logger.info(f"Данные успешно сохранены: {retail_chain} - {address} - {sale_date}")
            return True
        
        except Exception as e:
            logger.error(f"Ошибка при сохранении в базу данных для {retail_chain} - {address}: {e}")
            return False


    def save_store_location(self, data: Dict[str, Any]) -> bool:
        """
        Сохраняет полную информацию о ТТ с данными о продажах
        """
        try:
            # Получаем данные о продажах
            retail_chain = data.get('network') or data.get('retail_chain') or ''
            address = data.get('original_address') or data.get('address') or ''
            sales_data = self.get_sales_data(retail_chain, address, sale_date)

            
            # Объединяем данные
            full_data = {
                **data,
                **sales_data,
                'sales_quantity': sales_data['sales_quantity'],
                'sales_amount_rub': sales_data['sales_amount_rub'],
                'avg_sell_price': sales_data['avg_sell_price'],
                'avg_cost_price': sales_data['avg_cost_price']
            }
            
            # Сохраняем в базу
            return self.save_to_database(full_data)

        except Exception as e:
            logger.error(f"Ошибка при сохранении локации в БД: {e}")
            return False

    def get_location_info(self, address: str) -> Optional[Dict]:
        """Получение информации о местоположении с ограничением по России"""
        if not self.api_keys:
            logger.warning("API ключи не указаны. Геокодирование невозможно.")
            return None
            
        try:
            # Добавляем ограничение поиска по России
            address_with_country = f"{address}, Россия"
            encoded_address = urllib.parse.quote(address_with_country)
            
            # Формируем параметры запроса
            params = {
                'geocode': address_with_country,
                'format': 'json',
                'results': 5,
                'apikey': self.get_current_api_key(),
                'lang': 'ru_RU'
            }
            
            logger.info(f"Геокодируем адрес: {address} с ключом: {self.get_current_api_key()[:8]}...{self.get_current_api_key()[-4:]}")
            
            response = self.session.get(self.geocoder_url, params=params, timeout=15)
            
            # Проверяем статус ответа
            if response.status_code != 200:
                logger.error(f"Ошибка HTTP {response.status_code}: {response.text}")
                
                # Проверяем, не исчерпан ли лимит API
                if response.status_code == 403 or "limit" in response.text.lower():
                    logger.error("⚠️ Лимит API запросов исчерпан для текущего ключа!")
                    # Помечаем текущий ключ как невалидный на сегодня
                    self.mark_key_as_exhausted(self.get_current_api_key())
                    
                    if self.switch_to_next_key():
                        # Рекурсивно повторяем запрос с новым ключом
                        return self.get_location_info(address)
                    else:
                        logger.error("⚠️ Все ключи исчерпаны!")
                        return {"success": False, "api_limit_exceeded": True}
                    
                return None
                
            response.raise_for_status()
            
            geocode_data = response.json()
            
            # Проверяем наличие ошибки лимита в JSON ответе
            if (geocode_data.get('status') == 403 or 
                'limit' in str(geocode_data).lower()):
                logger.error("⚠️ Лимит API запросов исчерпан для текущего ключа!")
                # Помечаем текущий ключ как невалидный на сегодня
                self.mark_key_as_exhausted(self.get_current_api_key())
                
                if self.switch_to_next_key():
                    # Рекурсивно повторяем запрос с новым ключом
                    return self.get_location_info(address)
                else:
                    logger.error("⚠️ Все ключи исчерпаны!")
                    return {"success": False, "api_limit_exceeded": True}
            
            location_info = self._parse_geocode(geocode_data, address)
            
            if location_info:
                logger.info(f"Успешно обработан: {address}")
                return location_info
            else:
                logger.warning(f"Не удалось обработать адрес: {address}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Сетевая ошибка для адреса {address}: {e}")
            return None
        except Exception as e:
            logger.error(f"Ошибка обработки адреса {address}: {e}")
            return None

    def mark_key_as_exhausted(self, key: str):
        """Помечает ключ как исчерпанный на сегодня"""
        # Можно добавить логику для сохранения информации об исчерпанных ключах
        # Например, в базу данных или файл
        logger.warning(f"Ключ {key[:8]}...{key[-4:]} помечен как исчерпанный")


    def process_source_table(self, max_requests: int = 2000, sleep_between: float = 0.5) -> Dict[str, int]:
        """Обрабатывает новые адреса с конкретной датой продажи"""
        stats = {'fetched': 0, 'processed': 0, 'saved': 0, 'errors': 0,
                'api_requests': 0, 'api_limit_hit': False}

        try:
            rows = self.get_data_from_source_table()
            stats['fetched'] = len(rows)
            rows_to_process = rows[:max_requests]
            total_to_process = len(rows_to_process)
            logger.info(f"Найдено новых записей: {stats['fetched']}")
            logger.info(f"Будет обработано (лимит {max_requests}): {total_to_process}")
            if total_to_process == 0:
                return stats

            pbar = tqdm(total=total_to_process, desc="Обработка адресов", unit="адрес")
            for row in rows_to_process:
                try:
                    if stats['api_limit_hit']:
                        break

                    sale_date = row['sale_date']
                    retail_chain = row['retail_chain']
                    store_format = row.get('store_format', '')
                    address = row['address']
                    stats['processed'] += 1

                    pbar.set_postfix({'обработано': stats['processed'],
                                    'осталось': total_to_process - stats['processed'],
                                    'сохранено': stats['saved'],
                                    'ошибки': stats['errors'],
                                    'api_запросов': stats['api_requests']})
                    pbar.update(1)

                    geodata = self.get_location_info(address)
                    stats['api_requests'] += 1
                    
                    # Проверяем, не исчерпаны ли все ключи
                    if geodata and geodata.get('api_limit_exceeded'):
                        stats['api_limit_hit'] = True
                        logger.error("Все ключи исчерпаны, прекращаем обработку")
                        break

                    if geodata and geodata.get('success'):
                        # Используем данные геокодера
                        city = geodata.get('city')
                        federal_district = geodata.get('federal_district')
                        federal_subject = geodata.get('federal_subject')
                        coords = {'lat': geodata.get('lat'), 'lon': geodata.get('lon')}
                    else:
                        # Пропускаем запись, если не удалось получить геоданные
                        # и все ключи исчерпаны
                        if stats['api_limit_hit']:
                            logger.warning(f"Пропускаем адрес {address} из-за исчерпания ключей")
                            continue

                    data = {'sale_date': sale_date, 'network': retail_chain,
                            'retail_chain': retail_chain, 'format': store_format,
                            'original_address': address, 'address': address,
                            'city': city, 'region': federal_district,
                            'federal_district': federal_district,
                            'federal_subject': federal_subject,
                            'coordinates': coords}

                    saved = self.save_to_database(data)
                    if saved:
                        stats['saved'] += 1
                    else:
                        stats['errors'] += 1

                    time.sleep(sleep_between)

                except Exception as e_row:
                    logger.error(f"Ошибка при обработке строки {row}: {e_row}")
                    stats['errors'] += 1

            pbar.close()
            logger.info(f"Обработка завершена. API запросов: {stats['api_requests']}")
            return stats

        except Exception as e:
            logger.error(f"Ошибка при чтении исходной таблицы: {e}")
            stats['errors'] += 1
            return stats

    
    def _parse_geocode(self, data: Dict, original_address: str) -> Optional[Dict]:
        """Парсинг ответа геокодера с улучшенным определением регионов"""
        try:
            if not data or 'response' not in data:
                return None
                
            response = data['response']
            collection = response.get('GeoObjectCollection', {})
            
            if 'metaDataProperty' in collection:
                meta = collection['metaDataProperty']['GeocoderResponseMetaData']
                found = meta.get('found', 0)
                if found == 0:
                    return None
            
            features = collection.get('featureMember', [])
            
            if not features:
                return None
            
            for feature in features:
                geo_object = feature.get('GeoObject', {})
                
                if not geo_object:
                    continue
                
                meta_data = geo_object.get('metaDataProperty', {}).get('GeocoderMetaData', {})
                address_details = meta_data.get('Address', {})
                address_components = address_details.get('Components', [])
                
                is_russian = False
                federal_district = None
                federal_subject = None
                city = None
                region_name = None
                
                # Сначала ищем страну
                for component in address_components:
                    kind = component.get('kind', '')
                    name = component.get('name', '')
                    
                    if kind == 'country' and name == 'Россия':
                        is_russian = True
                        break
                
                if not is_russian:
                    continue
                    
                # Теперь ищем остальные компоненты
                for component in address_components:
                    kind = component.get('kind', '')
                    name = component.get('name', '')
                    
                    if kind == 'locality':
                        city = name
                    elif kind == 'province':
                        # Это субъект федерации (область, край, республика)
                        federal_subject = name
                    elif kind == 'area' and not federal_subject:
                        # Альтернативное название для региона
                        federal_subject = name
                    elif kind == 'region':
                        # Это может быть федеральный округ или другой регион
                        if 'федеральный округ' in name.lower():
                            federal_district = name
                        elif not federal_subject:
                            federal_subject = name
                
                # Если федеральный округ не найден, но найден субъект, определяем по словарю
                if not federal_district and federal_subject:
                    federal_district = self._find_federal_district(federal_subject)
                
                # Если город не найден, пытаемся извлечь из адреса
                if not city:
                    # Ищем в компонентах что-то похожее на город
                    for component in address_components:
                        if component.get('kind') in ['locality', 'district', 'area']:
                            city = component.get('name')
                            break
                
                # Координаты
                point = geo_object.get('Point', {})
                pos = point.get('pos')
                if not pos:
                    continue
                    
                lon, lat = map(float, pos.split())
                
                # Полный адрес
                full_address = meta_data.get('text', '')
                
                return {
                    'lat': lat,
                    'lon': lon,
                    'coordinates': (lat, lon),
                    'city': city or 'Неизвестно',
                    'federal_district': federal_district or 'Неизвестно',
                    'federal_subject': federal_subject or 'Неизвестно',
                    'full_address': full_address,
                    'success': True
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Ошибка парсинга геокодера: {e}")
            logger.debug(f"Ответ геокодера: {data}")
            return None
    
    def _find_federal_district(self, subject: str) -> str:
        """Поиск федерального округа по субъекту РФ"""
        subject_lower = subject.lower()
        for subject_name, district in self.subject_to_district.items():
            if subject_name in subject_lower:
                return district
        return 'Неизвестно'
    
    def safe_get_city_region(self, address: str) -> Dict:
        """Безопасное получение города и региона"""
        result = self.get_location_info(address)
        
        if result and result.get('success'):
            return {
                'city': result['city'],
                'region': result['federal_district'],
                'federal_subject': result['federal_subject'],
                'coordinates': result['coordinates'],
                'success': True
            }
        else:
            # Резервный метод: улучшенное извлечение из адреса
            city_region = self._extract_from_address(address)
            return {
                'city': city_region['city'],
                'region': city_region['region'],
                'federal_subject': city_region['federal_subject'],
                'coordinates': (0, 0),
                'success': False
            }
    
    def _extract_from_address(self, address: str) -> Dict:
        """Улучшенное извлечение города и региона из текста адреса"""
        # Приводим к нижнему регистру для удобства
        address_lower = address.lower()
        
        # Сначала пробуем найти регион в адресе
        region = 'Неизвестно'
        federal_subject = 'Неизвестно'
        
        # Список всех возможных субъектов РФ
        all_subjects = []
        for subjects in self.federal_districts.values():
            all_subjects.extend(subjects)
        
        # Ищем упоминание любого субъекта РФ в адресе
        for subject in all_subjects:
            if subject.lower() in address_lower:
                federal_subject = subject
                # Определяем федеральный округ по субъекту
                region = self._find_federal_district(subject)
                break
        
        # Если не нашли субъект, пробуем найти регион по ключевым словам
        if federal_subject == 'Неизвестно':
            region_keywords = {
                'респ': 'Республика',
                'обл': 'Область',
                'край': 'Край',
                'ао': 'Автономный округ'
            }
            
            for keyword, region_type in region_keywords.items():
                if keyword in address_lower:
                    # Извлекаем название региона
                    pattern = fr'([^,]+?{keyword})'
                    match = re.search(pattern, address, re.IGNORECASE)
                    if match:
                        federal_subject = match.group(1)
                        break
        
        # Извлечение города
        city = 'Неизвестно'
        patterns = [
            r'(?:г\.|город|гор\.)\s*([^,]+)',
            r',\s*([^,]+?)\s*(?:г|город|\(г\))',
            r'^([^,]+?),',
            r',\s*([^,]+?),',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, address, re.IGNORECASE)
            if match:
                potential_city = match.group(1).strip()
                # Проверяем, что это не регион и не улица
                if not any(word in potential_city.lower() for word in 
                        ['ул', 'улица', 'проспект', 'пр', 'площадь', 'пер', 'переулок', 
                        'респ', 'обл', 'край', 'ао', 'район', 'р-н']):
                    city = potential_city
                    break
        
        return {
            'city': city, 
            'region': region,
            'federal_subject': federal_subject
        }


def get_today_api_usage(self) -> int:
    """Получает количество API запросов, сделанных сегодня"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        sql = """
        SELECT COUNT(*) 
        FROM [Stage].[bi].[STORE_CHARACTERISTICS] 
        WHERE CAST(created_at AS DATE) = CAST(GETDATE() AS DATE)
        """
        
        cursor.execute(sql)
        count = cursor.fetchone()[0]
        return count
        
    except Exception as e:
        logger.error(f"Ошибка при получении статистики API: {e}")
        return 0

def main():
    print("🔍 Запуск обработки данных из БД")
    
    # Список API ключей
    API_KEYS = [
        '4eafaf6f-51c9-47d0-be01-cddf8e94f4a7',
        '18ffa901-3ca3-4490-9222-ed66046d64d7',
        '27b61e45-ccdd-4c16-b6c7-e9c6e38c01f7',
        '694470aa-33bb-49c8-a0ba-1be0e99ec787',
        '54bf3eb1-a2d7-400b-9928-acc90a2a5780',
        '22706d49-4f15-41d6-892b-cde7473200de',
        '2056b23c-648c-4952-ac7a-d5952575e7db',
        '4f0efc9d-e486-4952-983d-dd4847d599a8',
        '413dcd39-ba92-43a2-92e1-51cec7aa26cd',
        '57bbd123-1ee5-48e8-95d3-9207318b7450',
        'c81804b3-3b27-400e-8c8e-3c2d688d9d43',
        '08fc2bb0-4759-40ff-b507-48005ba26947',
        '7b730765-17f9-4eec-822b-839c92ad7cad'
    ]
    
    processor = YandexGeoProcessor(api_keys=API_KEYS)

    # Явный вывод информации о ключах
    print(f"🔑 Используется {len(API_KEYS)} ключей")
    for i, key in enumerate(API_KEYS):
        print(f"  {i+1}. {key[:8]}...{key[-4:]}")
    
    # Получаем количество записей для обработки
    rows_to_process = processor.get_data_from_source_table()
    total_records = len(rows_to_process)
    print(f"📋 Всего записей для обработки: {total_records}")
    
    # Обработка только новых записей, максимум 40000 API запросов
    stats = processor.process_source_table(
        max_requests=40000,  # Ограничение на API запросы
        sleep_between=0.1     # Пауза между запросами
    )
    
    print(f"\n📊 Статистика обработки:")
    print(f"   Всего новых записей: {stats['fetched']}")
    print(f"   Обработано: {stats['processed']}")
    print(f"   API запросов: {stats['api_requests']}")
    print(f"   Сохранено: {stats['saved']}")
    print(f"   Ошибок: {stats['errors']}")
    
    # Выводим сколько осталось обработать
    remaining = total_records - stats['processed']
    print(f"   Осталось обработать: {remaining}")
    
    if stats['api_limit_hit']:
        print("\n⚠️  Все ключи исчерпаны! Обработка прервана.")
    elif stats['api_requests'] >= 40000:
        print("\n⚠️  Достигнут лимит в 40000 API запросов. Запустите завтра для продолжения.")

if __name__ == "__main__":
    main()