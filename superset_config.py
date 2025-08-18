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
# Настройки баз данных
# ========================
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_data/superset.db"

# ========================
# ClickHouse конфигурация
# ========================
CLICKHOUSE_DB_URL = "clickhouse+native://default:password@clickhouse:9000/default"

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
# Инициализация приложения
# ========================


def init_app(app: Flask):
    # Установка SECRET_KEY из переменных окружения или файла
    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY') or 'your-default-secret-key-change-me'

    # Отключаем проблемные функции
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['ENCRYPTED_FIELD_KEY'] = os.environ.get('SECRET_KEY')[:32]

# Минимальные настройки ClickHouse
CLICKHOUSE_DB_URL = "clickhouse+native://default:@clickhouse:9000/default"