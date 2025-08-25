import osmnx as ox
import geopandas as gpd
import pandas as pd
import numpy as np
import requests
from typing import List, Dict, Any, Optional, Tuple
import time
import json
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import re
from unidecode import unidecode
import logging
import pyodbc
import threading
import os

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


class EnhancedOSMExtractor:
    def __init__(self):
        self.geolocator = Nominatim(user_agent="tt_analyzer_enhanced")
        self.geocode = RateLimiter(self.geolocator.geocode, min_delay_seconds=1)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'TT-Analyzer-Research/1.0 (research-project@example.com)'
        })

    def save_to_database(self, data: Dict[str, Any]) -> bool:
        """Сохраняет данные о торговой точке в базу данных"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Подготовка данных для вставки
            retail_chain = data.get('network', '')
            store_format = data.get('format', '')
            address = data.get('original_address', data.get('address', ''))
            lat = data.get('coordinates', {}).get('lat', 0)
            lon = data.get('coordinates', {}).get('lon', 0)
            area_m2 = data.get('area', data.get('estimated_area', 0))
            has_alcohol_department = 1 if data.get('has_alcohol', False) else 0
            has_snacks = 1 if data.get('has_grocery', False) else 0
            
            # SQL запрос для вставки данных
            sql = """
            INSERT INTO [Stage].[bi].[STORE_CHARACTERISTICS] 
            (retail_chain, store_format, address, lat, lon, area_m2, has_alcohol_department, has_snacks)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # Выполнение запроса
            cursor.execute(sql, 
                          retail_chain, 
                          store_format, 
                          address, 
                          lat, 
                          lon, 
                          area_m2, 
                          has_alcohol_department, 
                          has_snacks)
            
            conn.commit()
            logger.info(f"Данные успешно сохранены в базу для {retail_chain} - {address}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении в базу данных: {e}")
            return False

    def get_data_from_source_table(self, limit: int = 100) -> List[Dict]:
        """Получение данных из исходной таблицы"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            sql = f"""
            SELECT TOP {limit} retail_chain, store_format, address
            FROM [Stage].[bi].[ALL_DATA_COMPETITORS_MATERIALIZED]
            """
            
            cursor.execute(sql)
            rows = cursor.fetchall()
            
            result = []
            for row in rows:
                result.append({
                    'retail_chain': row.retail_chain,
                    'store_format': row.store_format,
                    'address': row.address
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Ошибка при получении данных из таблицы: {e}")
            return []

    def determine_network_subtype(self, network: str, address: str, store_format: str = None) -> str:
        """Определение подтипа сети на основе различных факторов"""
        # Если формат уже указан в исходных данных, используем его
        if store_format and store_format.strip():
            return store_format
        
        network_lower = network.lower()
        
        # Определение подтипа на основе названия сети
        subtype_mapping = {
            'пятерочка': 'магазин у дома',
            'перекресток': 'супермаркет',
            'магнит': 'магазин у дома',
            'лента': 'гипермаркет',
            'ашан': {
                'ашан сити': 'супермаркет',
                'ашан экспресс': 'магазин у дома',
                'default': 'гипермаркет'
            },
            'дикси': 'магазин у дома',
            'метро': 'cash&carry',
            'окей': 'гипермаркет',
            'виктория': 'супермаркет'
        }
        
        # Поиск в маппинге
        for key, value in subtype_mapping.items():
            if key in network_lower:
                if isinstance(value, dict):
                    # Для сетей с подтипами
                    for sub_key, sub_value in value.items():
                        if sub_key != 'default' and sub_key in network_lower:
                            return sub_value
                    return value.get('default', 'супермаркет')
                else:
                    return value
        
        # Если не нашли в маппинге, анализируем адрес
        address_lower = address.lower()
        
        # Определение по ключевым словам в адресе
        if any(word in address_lower for word in ['торговый центр', 'тц', 'молл', 'mall']):
            return 'торговый центр'
        elif any(word in address_lower for word in ['гипермаркет', 'hypermarket']):
            return 'гипермаркет'
        elif any(word in address_lower for word in ['супермаркет', 'supermarket']):
            return 'супермаркет'
        elif any(word in address_lower for word in ['у дома', 'у дома', 'продукты']):
            return 'магазин у дома'
        elif any(word in address_lower for word in ['дискаунтер', 'discount']):
            return 'дискаунтер'
        
        # По умолчанию
        return 'магазин у дома'

    def normalize_address(self, address: str) -> str:
        """Нормализация адреса для улучшения поиска"""
        # Приводим к нижнему регистру
        normalized = address.lower()

        # Заменяем сокращения
        replacements = {
            'г\.': 'город ',
            'с\.': 'село ',
            'ул\.': 'улица ',
            'пр\.': 'проспект ',
            'пер\.': 'переулок ',
            'д\.': 'дом ',
            'дом №': 'дом ',
            'обл\.': 'область',
            'край': '',
            'респ\.': 'республика '
        }

        for old, new in replacements.items():
            normalized = re.sub(old, new, normalized)

        # Удаляем лишние запятые и пробелы
        normalized = re.sub(r'\s+', ' ', normalized)
        normalized = re.sub(r',\s*,', ',', normalized)
        normalized = normalized.strip().strip(',')

        return normalized

    def extract_settlement(self, address: str) -> str:
        """Извлечение названия населенного пункта из адреса"""
        # Паттерны для извлечения населенного пункта
        patterns = [
            r',\s*([^,]+?\s*г[ород]*),',  # город
            r',\s*([^,]+?\s*с[ело]*),',  # село
            r',\s*([^,]+?\s*п[оселок]*),',  # поселок
            r',\s*([^,]+?\s*д[еревня]*),',  # деревня
        ]

        for pattern in patterns:
            match = re.search(pattern, address, re.IGNORECASE)
            if match:
                return match.group(1).strip()

        # Если не нашли по паттернам, берем часть после региона
        parts = address.split(',')
        if len(parts) >= 3:
            return parts[1].strip()

        return address

    def _search_by_name_address(self, network: str, address: str) -> List[Dict]:
        """Поиск по точному названию и адресу"""
        query = f"{network} {address}"
        return self._nominatim_search(query)

    def _search_by_settlement_and_name(self, network: str, address: str) -> List[Dict]:
        """Поиск по населенному пункту и названию сети"""
        settlement = self.extract_settlement(address)
        if settlement:
            query = f"{network} {settlement}"
            return self._nominatim_search(query)
        return []

    def _search_nearby_amenities(self, network: str, address: str) -> List[Dict]:
        """Улучшенный поиск nearby amenities с учетом специфики торговых сетей"""
        try:
            settlement = self.extract_settlement(address)
            if not settlement:
                return []

            location = self.geocode(settlement)
            if not location:
                return []

            # Более специфичные теги для поиска
            tags = {
                'shop': ['supermarket', 'convenience', 'department_store'],
                'brand': None  # Ищем любые бренды
            }

            gdf = ox.features.features_from_address(
                location.address,
                tags=tags,
                dist=15000  # Увеличиваем радиус до 15km
            )

            results = []
            network_lower = network.lower()

            for idx, row in gdf.iterrows():
                name = str(row.get('name', '')).lower()
                brand = str(row.get('brand', '')).lower()
                operator = str(row.get('operator', '')).lower()

                # Более гибкое сравнение названий
                match_criteria = [
                    network_lower in name,
                    network_lower in brand,
                    network_lower in operator,
                    any(network_lower in str(tag).lower() for tag in row.get('tags', []))
                ]

                if any(match_criteria):
                    geometry = row.geometry
                    if hasattr(geometry, 'centroid'):
                        centroid = geometry.centroid
                        lat, lon = centroid.y, centroid.x
                    else:
                        lat, lon = geometry.y, geometry.x

                    result = {
                        'osm_id': idx[1] if isinstance(idx, tuple) else None,
                        'osm_type': idx[0] if isinstance(idx, tuple) else 'way',
                        'name': row.get('name', ''),
                        'brand': row.get('brand', ''),
                        'operator': row.get('operator', ''),
                        'lat': lat,
                        'lon': lon,
                        'tags': {k: v for k, v in row.items() if pd.notna(v) and k != 'geometry'}
                    }
                    results.append(result)

            return results

        except Exception as e:
            logger.error(f"Ошибка в nearby search: {e}")
            return []

    def _search_with_overpass(self, network: str, address: str) -> List[Dict]:
        """Поиск через Overpass API"""
        try:
            settlement = self.extract_settlement(address)
            if not settlement:
                return []

            # Формируем Overpass запрос
            overpass_query = f"""
            [out:json];
            area["name"="{settlement}"]->.searchArea;
            (
              node["shop"](area.searchArea);
              way["shop"](area.searchArea);
              relation["shop"](area.searchArea);
            );
            out center;
            """

            response = self.session.get(
                "https://overpass-api.de/api/interpreter",
                params={'data': overpass_query},
                timeout=30
            )
            response.raise_for_status()

            data = response.json()
            results = []
            network_lower = network.lower()

            for element in data.get('elements', []):
                tags = element.get('tags', {})
                name = str(tags.get('name', '')).lower()
                brand = str(tags.get('brand', '')).lower()

                if network_lower in name or network_lower in brand:
                    # Определяем координаты
                    if 'lat' in element and 'lon' in element:
                        lat, lon = element['lat'], element['lon']
                    elif 'center' in element:
                        lat, lon = element['center']['lat'], element['center']['lon']
                    else:
                        continue

                    result = {
                        'osm_id': element['id'],
                        'osm_type': element['type'],
                        'name': tags.get('name', ''),
                        'brand': tags.get('brand', ''),
                        'lat': lat,
                        'lon': lon,
                        'tags': tags
                    }
                    results.append(result)

            return results

        except Exception as e:
            logger.error(f"Ошибка Overpass поиска: {e}")
            return []

    def _search_with_alternative_queries(self, network: str, address: str) -> List[Dict]:
        """Поиск с альтернативными формулировками запросов"""
        alternatives = [
            f"{network} {self.extract_settlement(address)}",
            f'магазин "{network}" {self.extract_settlement(address)}',
            f'{network} {address.split(",")[-1].strip()}',  # только улица и дом
            f'{network} {self.extract_settlement(address)} Алтайский край'
        ]

        all_results = []
        for query in alternatives:
            results = self._nominatim_search(query)
            if results:
                all_results.extend(results)
            time.sleep(0.3)

        return all_results

    def search_osm_multiple_strategies(self, network: str, address: str) -> List[Dict]:
        """Расширенный поиск с использованием дополнительных стратегий"""
        strategies = [
            self._search_by_name_address,
            self._search_by_settlement_and_name,
            self._search_nearby_amenities,
            self._search_with_overpass,
            self._search_with_alternative_queries
        ]

        all_results = []
        seen_ids = set()  # Для избежания дубликатов

        for strategy in strategies:
            try:
                results = strategy(network, address)
                if results:
                    for result in results:
                        # Создаем уникальный идентификатор
                        result_id = f"{result.get('osm_type', '')}_{result.get('osm_id', '')}"
                        if result_id not in seen_ids:
                            seen_ids.add(result_id)
                            all_results.append(result)

                    logger.info(f"Стратегия {strategy.__name__} найдена {len(results)} результатов")
            except Exception as e:
                logger.warning(f"Ошибка в стратегии {strategy.__name__}: {e}")
                continue

            # Небольшая задержка между стратегиями
            time.sleep(0.5)

        return all_results

    def _nominatim_search(self, query: str, limit: int = 10) -> List[Dict]:
        """Базовый поиск через Nominatim"""
        try:
            url = "https://nominatim.openstreetmap.org/search"
            params = {
                'q': query,
                'format': 'json',
                'limit': limit,
                'addressdetails': 1,
                'countrycodes': 'ru'  # ограничиваем поиск Россией
            }

            response = self.session.get(url, params=params, timeout=20)
            response.raise_for_status()

            data = response.json()
            return data

        except Exception as e:
            logger.error(f"Ошибка Nominatim поиска для '{query}': {e}")
            return []

    def get_detailed_osm_info(self, osm_id: int, osm_type: str) -> Optional[Dict]:
        """Получение детальной информации об объекте"""
        try:
            if osm_type == 'node':
                url = f"https://api.openstreetmap.org/api/0.6/node/{osm_id}.json"
            elif osm_type == 'way':
                url = f"https://api.openstreetmap.org/api/0.6/way/{osm_id}.json"
            elif osm_type == 'relation':
                url = f"https://api.openstreetmap.org/api/0.6/relation/{osm_id}.json"
            else:
                return None

            response = self.session.get(url, timeout=20)
            response.raise_for_status()

            data = response.json()
            if 'elements' in data and data['elements']:
                return data['elements'][0]

        except Exception as e:
            logger.error(f"Ошибка получения деталей OSM для {osm_type}/{osm_id}: {e}")

        return None

    def extract_tt_info(self, osm_data: Dict) -> Dict[str, Any]:
        """Улучшенное извлечение информации о торговой точке"""
        tags = osm_data.get('tags', {})
        address = osm_data.get('address', {})

        # Определяем формат магазина
        shop_type = tags.get('shop', '')
        format_mapping = {
            'supermarket': 'супермаркет',
            'hypermarket': 'гипермаркет',
            'convenience': 'магазин у дома',
            'discount': 'дискаунтер',
            'department_store': 'универмаг',
            'mall': 'торговый центр',
            'wholesale': 'оптовый'
        }
        tt_format = format_mapping.get(shop_type, 'другой')

        # Более точное определение алкоголя
        alcohol_tags = {
            'alcohol': ['yes', 'beer', 'wine', 'spirits'],
            'brewery': 'yes',
            'drink:beer': 'yes'
        }

        has_alcohol = False
        for tag, values in alcohol_tags.items():
            if tag in tags:
                if isinstance(values, list):
                    if tags[tag] in values:
                        has_alcohol = True
                        break
                else:
                    if tags[tag] == values:
                        has_alcohol = True
                        break

        # Определение бакалеи и снеков
        food_tags = ['bakery', 'deli', 'butcher', 'seafood', 'pastry']
        has_grocery = any(tag in tags for tag in food_tags)

        # Если нет специфичных теги, но есть общие категории
        if not has_grocery:
            grocery_categories = ['food', 'grocery', 'supermarket']
            has_grocery = any(cat in str(tags.values()).lower() for cat in grocery_categories)

        # Площадь с дополнительными способами определения
        area = None
        area_sources = ['area', 'floor_area', 'building:area']

        for source in area_sources:
            if source in tags:
                try:
                    area = float(tags[source])
                    break
                except (ValueError, TypeError):
                    continue

        # Определение этажности (может влиять на общую площадь)
        building_levels = tags.get('building:levels')
        if area is None and building_levels:
            try:
                levels = int(building_levels)
                # Примерная оценка площади на основе этажей
                area = levels * 400  # Средняя площадь этажа
            except (ValueError, TypeError):
                pass

        return {
            'osm_id': osm_data.get('osm_id'),
            'osm_type': osm_data.get('osm_type'),
            'name': tags.get('name', ''),
            'brand': tags.get('brand', ''),
            'address': address.get('display_name', ''),
            'format': tt_format,
            'shop_type': shop_type,
            'has_alcohol': has_alcohol,
            'has_grocery': has_grocery,  # Добавляем информацию о бакалее
            'area': area,
            'opening_hours': tags.get('opening_hours', ''),
            'coordinates': {
                'lat': float(osm_data.get('lat', 0)),
                'lon': float(osm_data.get('lon', 0))
            },
            'source': 'osm',
            'confidence': 'high' if all([tt_format != 'другой', area is not None]) else 'medium'
        }

    def get_coordinates_from_address(self, address: str) -> Dict[str, float]:
        """Надежное получение координат из адреса"""
        try:
            normalized_address = self.normalize_address(address)
            location = self.geocode(normalized_address)

            if location:
                return {'lat': location.latitude, 'lon': location.longitude}

            # Попробуем без нормализации
            location = self.geocode(address)
            if location:
                return {'lat': location.latitude, 'lon': location.longitude}

            # Последняя попытка - только населенный пункт
            settlement = self.extract_settlement(address)
            if settlement:
                location = self.geocode(settlement)
                if location:
                    return {'lat': location.latitude, 'lon': location.longitude}

        except Exception as e:
            logger.error(f"Ошибка геокодирования адреса {address}: {e}")

        return {'lat': 0, 'lon': 0}

    def manual_tt_estimation(self, network: str, address: str, store_format: str = None) -> Dict[str, Any]:
        """Расширенная ручная оценка параметров ТТ"""
        network_profiles = {
            'пятерочка': {
                'format': 'магазин у дома',
                'avg_area': 300,
                'has_alcohol': True,
                'has_grocery': True,
                'avg_check': 450
            },
            'перекресток': {
                'format': 'супермаркет',
                'avg_area': 800,
                'has_alcohol': True,
                'has_grocery': True,
                'avg_check': 1200
            },
            'магнит': {
                'format': 'магазин у дома',
                'avg_area': 250,
                'has_alcohol': False,
                'has_grocery': True,
                'avg_check': 350
            },
            'лента': {
                'format': 'гипермаркет',
                'avg_area': 2500,
                'has_alcohol': True,
                'has_grocery': True,
                'avg_check': 1800
            },
            'ашан': {
                'format': 'гипермаркет',
                'avg_area': 4000,
                'has_alcohol': True,
                'has_grocery': True,
                'avg_check': 2200,
                'subformats': {
                    'ашан сити': {'format': 'супермаркет', 'avg_area': 800},
                    'ашан экспресс': {'format': 'магазин у дома', 'avg_area': 300}
                }
            },
            'дикси': {
                'format': 'магазин у дома',
                'avg_area': 280,
                'has_alcohol': True,
                'has_grocery': True,
                'avg_check': 400
            }
        }

        network_lower = network.lower()
        profile = None
        subformat = None

        # Поиск основного профиля
        for key, value in network_profiles.items():
            if key in network_lower:
                profile = value.copy()
                break

        # Проверяем наличие подформатов
        if profile and 'subformats' in profile:
            for sub_name, sub_profile in profile['subformats'].items():
                if sub_name in network_lower:
                    profile.update(sub_profile)
                    break

        if profile:
            # Получаем координаты через геокодирование
            coords = self.get_coordinates_from_address(address)

            # Определяем формат (приоритет: переданный формат > профиль сети)
            final_format = store_format if store_format else profile['format']

            return {
                'network': network,
                'address': address,
                'format': final_format,
                'estimated_area': profile['avg_area'],
                'has_alcohol': profile['has_alcohol'],
                'has_grocery': profile['has_grocery'],
                'estimated_avg_check': profile.get('avg_check', 500),
                'coordinates': coords,
                'source': 'network_profile',
                'confidence': 'medium'
            }
        else:
            # Общая оценка для неизвестных сетей
            coords = self.get_coordinates_from_address(address)
            
            # Определяем формат
            final_format = store_format if store_format else self.determine_network_subtype(network, address)

            return {
                'network': network,
                'address': address,
                'format': final_format,
                'estimated_area': 200,
                'has_alcohol': True,
                'has_grocery': True,
                'estimated_avg_check': 500,
                'coordinates': coords,
                'source': 'generic_estimate',
                'confidence': 'low'
            }

    def process_address(self, network: str, address: str, store_format: str = None) -> Dict[str, Any]:
        """Обработка одного адреса"""
        logger.info(f"Обработка: {network} - {address}")

        # Пытаемся найти в OSM
        osm_results = self.search_osm_multiple_strategies(network, address)

        if osm_results:
            # Берем первый результат (наиболее релевантный)
            osm_data = osm_results[0]

            # Получаем детальную информацию
            detailed_info = self.get_detailed_osm_info(osm_data['osm_id'], osm_data['osm_type'])
            if detailed_info:
                osm_data.update(detailed_info)

            # Извлекаем информацию о ТТ
            tt_info = self.extract_tt_info(osm_data)
            tt_info['network'] = network
            tt_info['original_address'] = address
            tt_info['source'] = 'osm'

            # Сохраняем в базу данных
            self.save_to_database(tt_info)

            return tt_info
        else:
            # Если не нашли в OSM, используем ручную оценку
            logger.warning(f"Не найдено в OSM: {network} - {address}. Использую оценку.")
            manual_info = self.manual_tt_estimation(network, address, store_format)
            
            # Сохраняем в базу данных
            self.save_to_database(manual_info)
            
            return manual_info

    def process_batch_from_database(self, batch_size: int = 100):
        """Обработка пакета данных из исходной таблицы"""
        try:
            # Получаем данные из исходной таблицы
            source_data = self.get_data_from_source_table(batch_size)
            
            logger.info(f"Найдено {len(source_data)} записей для обработки")
            
            for i, item in enumerate(source_data):
                retail_chain = item['retail_chain']
                address = item['address']
                store_format = item['store_format']
                
                logger.info(f"Обрабатываем запись {i+1}/{len(source_data)}: {retail_chain} - {address}")
                
                try:
                    # Обрабатываем адрес
                    self.process_address(retail_chain, address, store_format)
                    
                    # Небольшая задержка между запросами
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Ошибка при обработке {retail_chain} - {address}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Ошибка при пакетной обработке: {e}")


# Пример использования
def main():
    extractor = EnhancedOSMExtractor()

    # Обработка пакета данных из базы
    extractor.process_batch_from_database(100)

    # Для тестирования одного адреса
    # test_address = "липецкая обл, липецк г, свиридова и.в. ул, дом № 22,корпус 1,помещение 1"
    # test_network = "Магнит"
    # result = extractor.process_address(test_network, test_address)
    # print("Результат обработки:")
    # print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()