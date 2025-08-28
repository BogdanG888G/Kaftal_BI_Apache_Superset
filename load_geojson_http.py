import requests
import json
import os

def load_geojson_to_superset():
    # Настройки Superset
    SUPERSET_HOST = os.getenv('SUPERSET_HOST', 'superset')
    SUPERSET_PORT = os.getenv('SUPERSET_PORT', '8088')
    SUPERSET_USERNAME = os.getenv('SUPERSET_USERNAME', 'admin')
    SUPERSET_PASSWORD = os.getenv('SUPERSET_PASSWORD', 'admin123')
    
    BASE_URL = f"http://{SUPERSET_HOST}:{SUPERSET_PORT}"
    
    # Путь к GeoJSON файлу
    GEOJSON_PATH = "/app/superset_data/maps/Russia_regions.geojson"
    
    # 1. Авторизация в Superset
    session = requests.Session()
    login_url = f"{BASE_URL}/api/v1/security/login"
    
    login_payload = {
        "username": SUPERSET_USERNAME,
        "password": SUPERSET_PASSWORD,
        "provider": "db",
        "refresh": True
    }
    
    try:
        response = session.post(login_url, json=login_payload)
        response.raise_for_status()
        print("✅ Успешная авторизация в Superset")
    except Exception as e:
        print(f"❌ Ошибка авторизации: {e}")
        return
    
    # 2. Чтение GeoJSON файла
    try:
        with open(GEOJSON_PATH, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        print("✅ GeoJSON файл прочитан успешно")
    except Exception as e:
        print(f"❌ Ошибка чтения GeoJSON: {e}")
        return
    
    # 3. Загрузка геоданных через API
    geojson_url = f"{BASE_URL}/api/v1/geojson/"
    
    payload = {
        "name": "Russia_regions",
        "geo_json": geojson_data,
        "properties": {"description": "Регионы России"}
    }
    
    try:
        response = session.post(geojson_url, json=payload)
        if response.status_code == 201:
            print("✅ GeoJSON успешно загружен в Superset!")
            print(f"ID геоданных: {response.json()['id']}")
        else:
            print(f"❌ Ошибка загрузки: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Ошибка при загрузке: {e}")

if __name__ == "__main__":
    load_geojson_to_superset()