

from flask import Flask
import os

# ========================
# Безопасность
# ========================

# В superset_config.py
from superset.typing import DashboardMeta
FEATURE_FLAGS = {
    "ALLOW_FULL_JSONP": True,
    "DASHBOARD_CACHE": True,
}

# Используйте OpenStreetMap вместо Mapbox
DECK_GL_MAP_STYLE = "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json"

SECRET_KEY = "Icgez+z6E/2YkbSzBS2s4ZHlMCYhgOelN/oqEE6U8mH2qE8bltzHU2Z2"
WTF_CSRF_ENABLED = True
SESSION_COOKIE_SECURE = False
ENCRYPTED_FIELD_KEY = SECRET_KEY[:32]

# ========================
# Языковые настройки
# ========================
BABEL_DEFAULT_LOCALE = 'ru'
LANGUAGES = {
    'en': {'flag': 'us', 'name': 'English'},
    'ru': {'flag': 'ru', 'name': 'Russian'}
}

# ========================
# Настройки баз данных
# ========================
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_data/superset.db"
SQLALCHEMY_TRACK_MODIFICATIONS = False

# ========================
# ClickHouse конфигурация
# ========================
CLICKHOUSE_DATABASE_URI = "clickhouse+http://admin:123@clickhouse:8123/default"

# ========================
# Настройки производительности
# ========================
SUPERSET_WEBSERVER_TIMEOUT = 300
SUPERSET_FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "GLOBAL_ASYNC_QUERIES": True,
}

# ========================
# Тест подключения к ClickHouse
# ========================
def test_clickhouse_connection():
    try:
        from sqlalchemy import create_engine
        
        test_urls = [
            "clickhouse+http://admin:123@clickhouse:8123/default",
            "clickhouse://admin:123@clickhouse:8123/default"
        ]
        
        for url in test_urls:
            try:
                engine = create_engine(url)
                with engine.connect() as conn:
                    result = conn.execute('SELECT 1 AS test_value')
                    print(f"✓ ClickHouse connection SUCCESS: {result.scalar()}")
                    return True
            except Exception as e:
                print(f"✗ ClickHouse connection FAILED with {url}: {str(e)[:200]}")
        
        return False
        
    except Exception as e:
        print(f"✗ ClickHouse test setup failed: {e}")
        return False

# ========================
# Инициализация приложения
# ========================
def init_app(app: Flask):
    # Безопасность
    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', SECRET_KEY)
    app.config['ENCRYPTED_FIELD_KEY'] = os.environ.get('ENCRYPTED_FIELD_KEY', ENCRYPTED_FIELD_KEY)
    app.config['WTF_CSRF_ENABLED'] = WTF_CSRF_ENABLED
    app.config['SESSION_COOKIE_SECURE'] = SESSION_COOKIE_SECURE
    
    # Базы данных
    app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('SQLALCHEMY_DATABASE_URI', SQLALCHEMY_DATABASE_URI)
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = SQLALCHEMY_TRACK_MODIFICATIONS
    
    # Язык
    app.config['BABEL_DEFAULT_LOCALE'] = BABEL_DEFAULT_LOCALE
    app.config['LANGUAGES'] = LANGUAGES
    
    # Производительность
    app.config['SUPERSET_WEBSERVER_TIMEOUT'] = SUPERSET_WEBSERVER_TIMEOUT
    app.config['SUPERSET_FEATURE_FLAGS'] = SUPERSET_FEATURE_FLAGS
    
    # Тестируем подключение
    try:
        connection_test = test_clickhouse_connection()
        if connection_test:
            print("✓ ClickHouse ready for use")
        else:
            print("⚠ ClickHouse connection failed")
    except Exception as e:
        print(f"⚠ ClickHouse test skipped: {e}")
    
    print("✓ Superset configuration initialized")

# ========================
# Дополнительные настройки
# ========================
PREVENT_UNSAFE_DB_CONNECTIONS = False
ENABLE_TIME_ROTATE = False
LOG_LEVEL = 'INFO'

# ========================
# Принудительная регистрация драйвера (после инициализации)
# ========================
def register_clickhouse_driver():
    try:
        # Простая регистрация через импорт
        import clickhouse_sqlalchemy
        print("✓ ClickHouse SQLAlchemy driver loaded")
        return True
    except ImportError as e:
        print(f"✗ ClickHouse driver not available: {e}")
        return False

# Запускаем регистрацию при загрузке модуля
register_clickhouse_driver()