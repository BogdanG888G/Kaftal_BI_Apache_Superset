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
import hashlib

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
    
def generate_address_hash(address: str) -> str:
    """Генерация хеша адреса для уникальности"""
    return hashlib.sha256(address.encode('utf-8')).hexdigest()

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
        """Переключение на следующий API ключ"""
        if not self.api_keys:
            return False
        
        original_index = self.current_key_index
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        
        if self.current_key_index == original_index:
            logger.error("Все ключи перебраны, лимит исчерпан!")
            return False
            
        logger.info(f"Переключились на ключ: {self.api_keys[self.current_key_index][:8]}...")
        return True

    def get_sales_data(self, retail_chain: str, address: str, sale_date: date) -> Dict[str, Any]:
        """Получение данных о продажах из исходной таблицы за конкретную дату"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # ИСПРАВЛЕННАЯ СТРОКА - убрал лишние скобки в названии таблицы
            sql = """
            SELECT 
                SUM(sales_quantity) as total_quantity,
                SUM(sales_amount_rub) as total_amount,
                AVG(avg_sell_price) as avg_sell,
                AVG(avg_cost_price) as avg_cost
            FROM [Stage].[bi].[ALL_DATA_COMPETITORS_CHIPS]
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
        """Получение новых записей с sale_date - ТОЛЬКО с продажами"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Ищем только записи с продажами, которых нет в STORE_CHARACTERISTICS
            sql = """
            SELECT DISTINCT 
                adc.sale_date, 
                adc.retail_chain, 
                adc.store_format, 
                adc.address
            FROM [Stage].[bi].[ALL_DATA_COMPETITORS_CHIPS] adc
            LEFT JOIN [Stage].[bi].[STORE_CHARACTERISTICS] sc 
                ON sc.retail_chain = adc.retail_chain 
                AND sc.address = adc.address
                AND sc.sale_date = adc.sale_date
            WHERE adc.retail_chain IS NOT NULL 
                AND adc.address IS NOT NULL
                AND adc.sales_quantity > 0
                AND adc.sales_amount_rub > 0
                AND sc.retail_chain IS NULL
            OPTION (MAXDOP 1)
            """
            cursor.execute(sql)
            rows = cursor.fetchall()
            
            result = []
            for row in rows:
                result.append({
                    'sale_date': row.sale_date, 
                    'retail_chain': row.retail_chain,
                    'store_format': row.store_format, 
                    'address': row.address
                })
            
            logger.info(f"Найдено {len(result)} новых записей с продажами")
            return result
            
        except Exception as e:
            logger.error(f"Ошибка при получении данных из таблицы: {e}")
            return []

    def update_existing_stores_sales(self) -> int:
        """Обновление данных о продажах в существующих записях STORE_CHARACTERISTICS"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            sql = """
            UPDATE sc
            SET 
                sales_quantity = sales_data.total_quantity,
                sales_amount_rub = sales_data.total_amount,
                avg_sell_price = sales_data.avg_sell,
                avg_cost_price = sales_data.avg_cost,
                created_at = GETDATE()
            FROM [Stage].[bi].[STORE_CHARACTERISTICS] sc
            INNER JOIN (
                SELECT 
                    retail_chain,
                    address,
                    sale_date,
                    SUM(sales_quantity) as total_quantity,
                    SUM(sales_amount_rub) as total_amount,
                    AVG(avg_sell_price) as avg_sell,
                    AVG(avg_cost_price) as avg_cost
                FROM [Stage].[bi].[ALL_DATA_COMPETITORS_CHIPS]
                WHERE sales_quantity > 0 AND sales_amount_rub > 0
                GROUP BY retail_chain, address, sale_date
            ) sales_data ON sc.retail_chain = sales_data.retail_chain 
                AND sc.address = sales_data.address
                AND sc.sale_date = sales_data.sale_date
            WHERE sc.sales_quantity = 0  -- обновляем только те, у кого продажи = 0
            """
            
            cursor.execute(sql)
            updated_count = cursor.rowcount
            conn.commit()
            
            logger.info(f"Обновлено записей с продажами: {updated_count}")
            return updated_count
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении продаж: {e}")
            return 0

    def get_store_type(self, network: str, format_type: str) -> str:
        """Определение типа магазина на основе сети и формата"""
        network_lower = network.lower()
        format_lower = format_type.lower() if format_type else 'default'
        
        if network_lower in self.store_type_mapping:
            network_types = self.store_type_mapping[network_lower]
            if format_lower in network_types:
                return network_types[format_lower]
            return network_types.get('default', 'Магазин у дома')
        
        return self.store_type_mapping['default'].get(format_lower, 
                    self.store_type_mapping['default']['default'])

    def get_area_from_range(self, network: str, format_type: str) -> float:
        """Получение площади на основе диапазона для сети и формата"""
        network_lower = network.lower()
        format_lower = format_type.lower() if format_type else 'default'
        
        if network_lower in self.area_ranges:
            network_ranges = self.area_ranges[network_lower]
            if format_lower in network_ranges:
                area_range = network_ranges[format_lower]
            else:
                area_range = network_ranges.get('default', (200, 400))
        else:
            default_ranges = self.area_ranges['default']
            if format_lower in default_ranges:
                area_range = default_ranges[format_lower]
            else:
                area_range = default_ranges['default']
        
        return int(np.random.uniform(area_range[0], area_range[1]))

    def save_to_database(self, data: Dict[str, Any]) -> bool:
        """Сохраняет данные о торговой точке в базу данных"""
        retail_chain = data.get('retail_chain', '')
        store_format = data.get('store_format', '')
        address = data.get('address', '')
        sale_date = data.get('sale_date') or date.today()

        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Проверяем существование записи
            check_sql = """
            SELECT COUNT(*) FROM [Stage].[bi].[STORE_CHARACTERISTICS] 
            WHERE retail_chain = ? AND address = ? AND sale_date = ?
            """
            cursor.execute(check_sql, retail_chain, address, sale_date)
            count = cursor.fetchone()[0]
            
            if count > 0:
                logger.info(f"Запись уже существует: {retail_chain} - {address} - {sale_date}")
                return False

            # Получаем данные о продажах
            sales_data = self.get_sales_data(retail_chain, address, sale_date)
            
            # Получаем геоданные
            geodata = self.get_location_info(address)
            
            if geodata and geodata.get('success'):
                city = geodata.get('city', 'Неизвестно')
                federal_district = geodata.get('federal_district', 'Неизвестно')
                federal_subject = geodata.get('federal_subject', 'Неизвестно')
                lat = geodata.get('lat', 0)
                lon = geodata.get('lon', 0)
            else:
                # Резервный метод извлечения данных из адреса
                extracted = self._extract_from_address(address)
                city = extracted['city']
                federal_district = extracted['region']
                federal_subject = extracted['federal_subject']
                lat = 0
                lon = 0

            area_m2 = self.get_area_from_range(retail_chain, store_format)
            has_alcohol_department = 1  # Предполагаем, что есть
            has_snacks = 1  # Предполагаем, что есть
            store_type = self.get_store_type(retail_chain, store_format)
            address_hash = generate_address_hash(address)

            sql = """
            INSERT INTO [Stage].[bi].[STORE_CHARACTERISTICS] 
            (retail_chain, store_format, store_type, address, sale_date, city, 
            federal_district, federal_subject,
            sales_quantity, sales_amount_rub, avg_sell_price, avg_cost_price, 
            lat, lon, area_m2, has_alcohol_department, has_snacks, created_at, address_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?)
            """
            
            cursor.execute(sql, 
                        retail_chain, store_format, store_type, address, sale_date,
                        city, federal_district, federal_subject,
                        sales_data['sales_quantity'], sales_data['sales_amount_rub'],
                        sales_data['avg_sell_price'], sales_data['avg_cost_price'],
                        lat, lon, area_m2, has_alcohol_department, has_snacks, address_hash)
            conn.commit()
            
            logger.info(f"Данные успешно сохранены: {retail_chain} - {address} - {sale_date}")
            return True
        
        except Exception as e:
            logger.error(f"Ошибка при сохранении в базу данных для {retail_chain} - {address}: {e}")
            return False

    def get_location_info(self, address: str) -> Optional[Dict]:
        """Получение информации о местоположении с ограничением по России"""
        if not self.api_keys:
            logger.warning("API ключи не указаны. Геокодирование невозможно.")
            return None
            
        max_retries = len(self.api_keys) * 2  # Двойной запас попыток
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                address_with_country = f"{address}, Россия"
                encoded_address = urllib.parse.quote(address_with_country)
                
                params = {
                    'geocode': address_with_country,
                    'format': 'json',
                    'results': 5,
                    'apikey': self.get_current_api_key(),
                    'lang': 'ru_RU'
                }
                
                logger.info(f"Геокодируем адрес: {address}")
                
                response = self.session.get(self.geocoder_url, params=params, timeout=15)
                
                if response.status_code != 200:
                    logger.error(f"Ошибка HTTP {response.status_code}")
                    
                    if response.status_code == 403 or "limit" in response.text.lower():
                        logger.warning("Лимит API исчерпан для текущего ключа")
                        if not self.switch_to_next_key():
                            return {"success": False, "api_limit_exceeded": True}
                        retry_count += 1
                        time.sleep(1)
                        continue
                    
                    return None
                    
                response.raise_for_status()
                geocode_data = response.json()
                
                # Проверяем лимит в JSON ответе
                if (geocode_data.get('status') == 403 or 
                    'limit' in str(geocode_data).lower()):
                    logger.warning("Лимит API исчерпан для текущего ключа")
                    if not self.switch_to_next_key():
                        return {"success": False, "api_limit_exceeded": True}
                    retry_count += 1
                    time.sleep(1)
                    continue
                
                location_info = self._parse_geocode(geocode_data, address)
                
                if location_info:
                    logger.info(f"Успешно обработан: {address}")
                    return location_info
                else:
                    logger.warning(f"Не удалось обработать адрес: {address}")
                    return None
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Сетевая ошибка для адреса {address}: {e}")
                retry_count += 1
                time.sleep(2)
            except Exception as e:
                logger.error(f"Ошибка обработки адреса {address}: {e}")
                return None
        
        logger.error(f"Превышено максимальное количество попыток для адреса: {address}")
        return None

    def _parse_geocode(self, data: Dict, original_address: str) -> Optional[Dict]:
        """Парсинг ответа геокодера"""
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
                
                # Проверяем страну
                for component in address_components:
                    kind = component.get('kind', '')
                    name = component.get('name', '')
                    
                    if kind == 'country' and name == 'Россия':
                        is_russian = True
                        break
                
                if not is_russian:
                    continue
                    
                # Ищем остальные компоненты
                for component in address_components:
                    kind = component.get('kind', '')
                    name = component.get('name', '')
                    
                    if kind == 'locality':
                        city = name
                    elif kind == 'province':
                        federal_subject = name
                    elif kind == 'area' and not federal_subject:
                        federal_subject = name
                    elif kind == 'region':
                        if 'федеральный округ' in name.lower():
                            federal_district = name
                        elif not federal_subject:
                            federal_subject = name
                
                # Определяем федеральный округ по субъекту
                if not federal_district and federal_subject:
                    federal_district = self._find_federal_district(federal_subject)
                
                # Координаты
                point = geo_object.get('Point', {})
                pos = point.get('pos')
                if not pos:
                    continue
                    
                lon, lat = map(float, pos.split())
                
                return {
                    'lat': lat,
                    'lon': lon,
                    'city': city or 'Неизвестно',
                    'federal_district': federal_district or 'Неизвестно',
                    'federal_subject': federal_subject or 'Неизвестно',
                    'success': True
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Ошибка парсинга геокодера: {e}")
            return None
    
    def _find_federal_district(self, subject: str) -> str:
        """Поиск федерального округа по субъекту РФ"""
        subject_lower = subject.lower()
        for subject_name, district in self.subject_to_district.items():
            if subject_name in subject_lower:
                return district
        return 'Неизвестно'
    
    def _extract_from_address(self, address: str) -> Dict:
        """Извлечение города и региона из текста адреса"""
        address_lower = address.lower()
        
        region = 'Неизвестно'
        federal_subject = 'Неизвестно'
        city = 'Неизвестно'
        
        # Ищем субъект РФ в адресе
        all_subjects = []
        for subjects in self.federal_districts.values():
            all_subjects.extend(subjects)
        
        for subject in all_subjects:
            if subject.lower() in address_lower:
                federal_subject = subject
                region = self._find_federal_district(subject)
                break
        
        # Извлечение города
        patterns = [
            r'(?:г\.|город|гор\.)\s*([^,]+)',
            r',\s*([^,]+?)\s*(?:г|город|\(г\))',
            r'^([^,]+?),',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, address, re.IGNORECASE)
            if match:
                potential_city = match.group(1).strip()
                if not any(word in potential_city.lower() for word in 
                        ['ул', 'улица', 'проспект', 'пр', 'площадь', 'пер', 'переулок']):
                    city = potential_city
                    break
        
        return {
            'city': city, 
            'region': region,
            'federal_subject': federal_subject
        }

    def process_source_table(self, max_requests: int = 2000, sleep_between: float = 0.5) -> Dict[str, int]:
        """Обрабатывает новые адреса с конкретной датой продажи"""
        stats = {
            'fetched': 0, 
            'processed': 0, 
            'saved': 0, 
            'errors': 0,
            'api_requests': 0, 
            'api_limit_hit': False
        }

        try:
            rows = self.get_data_from_source_table()
            stats['fetched'] = len(rows)
            rows_to_process = rows[:max_requests]
            total_to_process = len(rows_to_process)
            
            logger.info(f"Найдено новых записей с продажами: {stats['fetched']}")
            logger.info(f"Будет обработано (лимит {max_requests}): {total_to_process}")
            
            if total_to_process == 0:
                return stats

            pbar = tqdm(total=total_to_process, desc="Обработка адресов", unit="адрес")
            
            for row in rows_to_process:
                if stats['api_limit_hit']:
                    break

                try:
                    sale_date = row['sale_date']
                    retail_chain = row['retail_chain']
                    store_format = row.get('store_format', '')
                    address = row['address']
                    
                    stats['processed'] += 1

                    pbar.set_postfix({
                        'обработано': stats['processed'],
                        'осталось': total_to_process - stats['processed'],
                        'сохранено': stats['saved'],
                        'ошибки': stats['errors'],
                        'api_запросов': stats['api_requests']
                    })
                    pbar.update(1)

                    # Сохраняем данные
                    data = {
                        'sale_date': sale_date,
                        'retail_chain': retail_chain,
                        'store_format': store_format,
                        'address': address
                    }
                    
                    saved = self.save_to_database(data)
                    if saved:
                        stats['saved'] += 1
                    else:
                        stats['errors'] += 1

                    stats['api_requests'] += 1
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

    # Информация о ключах
    print(f"🔑 Используется {len(API_KEYS)} ключей")
    
    # 1. Сначала обновляем существующие записи с продажами
    print("🔄 Обновление данных о продажах в существующих магазинах...")
    updated_count = processor.update_existing_stores_sales()
    print(f"✅ Обновлено записей с продажами: {updated_count}")
    
    # 2. Затем обрабатываем новые магазины
    print("🔍 Поиск новых магазинов с продажами...")
    rows_to_process = processor.get_data_from_source_table()
    total_records = len(rows_to_process)
    print(f"📋 Всего новых записей для обработки: {total_records}")
    
    if total_records > 0:
        stats = processor.process_source_table(
            max_requests=40000,
            sleep_between=0.1
        )
        
        print(f"\n📊 Статистика обработки:")
        print(f"   Всего новых записей: {stats['fetched']}")
        print(f"   Обработано: {stats['processed']}")
        print(f"   API запросов: {stats['api_requests']}")
        print(f"   Сохранено: {stats['saved']}")
        print(f"   Ошибок: {stats['errors']}")
        
        remaining = total_records - stats['processed']
        print(f"   Осталось обработать: {remaining}")
        
        if stats['api_limit_hit']:
            print("\n⚠️  Все ключи исчерпаны! Обработка прервана.")
        elif stats['api_requests'] >= 40000:
            print("\n⚠️  Достигнут лимит в 40000 API запросов. Запустите завтра для продолжения.")
    else:
        print("✅ Новых магазинов для обработки нет")

if __name__ == "__main__":
    main()