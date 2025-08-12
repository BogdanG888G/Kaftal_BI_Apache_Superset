# -*- coding: utf-8 -*-
import os


# ==================== Безопасность ====================
SECRET_KEY = "Icgez+z6E/2YkbSzBS2s4ZHlMCYhgOelN/oqEE6U8mH2qE8bltzHU2Z2"

# ============ Основная база данных Superset ============
# Для SQLite (текущая конфигурация)
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_data/superset.db"

# Для MSSQL (альтернативный вариант)
# SQLALCHEMY_DATABASE_URI = (
#     "mssql+pyodbc://username:password@host.docker.internal:1433/SupersetDB"
#     "?driver=ODBC+Driver+17+for+SQL+Server"
#     "&Encrypt=no&TrustServerCertificate=yes"
# )

# ============ Внешние подключения (доп. базы MSSQL) ============
ADDITIONAL_DATABASE_CONNECTIONS = {
    "MSSQL_TEST": {
        "sqlalchemy_uri": (
            "mssql+pyodbc://airflow_agent:123@host.docker.internal/Test"
            "?driver=ODBC+Driver+17+for+SQL+Server"
            "&Encrypt=yes&TrustServerCertificate=yes"
        ),
        "engine_params": {"connect_args": {"timeout": 30}},
    },
}

# ==================== Доп. настройки ====================
SUPERSET_ENV = "production"
BABEL_DEFAULT_LOCALE = "ru"
LANGUAGES = {
    "en": {"flag": "us", "name": "English"},
    "ru": {"flag": "ru", "name": "Russian"},
}

# Конфигурация кэширования
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 86400,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_URL': 'redis://redis:6379/0'
}

# Конфигурация Celery
CELERY_CONFIG = {
    "broker_url": "redis://redis:6379/0",
    "result_backend": "redis://redis:6379/0",
}

# Отключение предупреждений (опционально)
SUPPRESS_WARNINGS = True