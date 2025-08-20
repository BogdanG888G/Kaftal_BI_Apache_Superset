from flask import Flask
from superset.superset_typing import FlaskResponse
import os

# ========================
# Безопасность
# ========================
SECRET_KEY = "Icgez+z6E/2YkbSzBS2s4ZHlMCYhgOelN/oqEE6U8mH2qE8bltzHU2Z2"
WTF_CSRF_ENABLED = True
SESSION_COOKIE_SECURE = False  # Для разработки

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

# ========================
# ClickHouse конфигурация
# ========================
# Основная строка подключения (используем HTTP протокол)
CLICKHOUSE_DATABASE_URI = "clickhouse+http://admin:123@clickhouse:8123/default"

# Альтернативные варианты (раскомментируйте если нужно)
# CLICKHOUSE_DATABASE_URI = "clickhouse+native://admin:123@clickhouse:9000/default"
# CLICKHOUSE_DATABASE_URI = "clickhouse://admin:123@clickhouse:8123/default"

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
# Дополнительные настройки SQLAlchemy
# ========================
SQLALCHEMY_TRACK_MODIFICATIONS = False
SQLALCHEMY_ECHO = False  # Для отладки установите True

# ========================
# Настройки подключения к базам данных
# ========================
# Разрешаем подключение к различным БД
PREVENT_UNSAFE_DB_CONNECTIONS = False

# ========================
# Инициализация приложения
# ========================
def init_app(app: Flask):
    # Установка SECRET_KEY
    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY') or SECRET_KEY
    
    # Ключ для шифрования полей
    app.config['ENCRYPTED_FIELD_KEY'] = os.environ.get('SECRET_KEY')[:32] if os.environ.get('SECRET_KEY') else SECRET_KEY[:32]
    
    # Языковые настройки
    app.config['BABEL_DEFAULT_LOCALE'] = BABEL_DEFAULT_LOCALE
    app.config['LANGUAGES'] = LANGUAGES
    
    # Настройки производительности
    app.config['SUPERSET_WEBSERVER_TIMEOUT'] = SUPERSET_WEBSERVER_TIMEOUT
    app.config['SUPERSET_FEATURE_FLAGS'] = SUPERSET_FEATURE_FLAGS
    
    # SQLAlchemy настройки
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = SQLALCHEMY_TRACK_MODIFICATIONS
    
    print("Superset configuration loaded successfully")

# ========================
# Регистрация драйверов БД
# ========================
# Явно регистрируем ClickHouse драйвер
try:
    from superset.db_engine_specs.clickhouse import ClickHouseEngineSpec
    # Принудительно добавляем в доступные драйверы
    DATABASE_DRIVERS = {
        'clickhouse': ClickHouseEngineSpec
    }
    print("ClickHouse driver registered successfully")
except ImportError as e:
    print(f"Warning: Could not import ClickHouseEngineSpec: {e}")

# ========================
# Переменные для тестирования
# ========================
if __name__ == "__main__":
    print("Testing ClickHouse connection...")
    try:
        from sqlalchemy import create_engine
        engine = create_engine(CLICKHOUSE_DATABASE_URI)
        with engine.connect() as conn:
            result = conn.execute('SELECT 1')
            print(f"Connection successful: {result.scalar()}")
    except Exception as e:
        print(f"Connection failed: {e}")