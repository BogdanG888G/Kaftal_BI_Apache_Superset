#!/bin/bash

set -e

# Генерация нового SECRET_KEY если его нет
if [ ! -f ~/.superset_secret_key ]; then
    echo "Generating new SECRET_KEY"
    python -c "import secrets; print(secrets.token_urlsafe(64))" > ~/.superset_secret_key
fi
export SECRET_KEY=$(cat ~/.superset_secret_key)

# Инициализация базы данных
superset db upgrade

# Создание администратора (только при первом запуске)
if [ ! -f ~/.superset_init_complete ]; then
    superset fab create-admin \
        --username admin \
        --firstname Admin \
        --lastname User \
        --email admin@admin.com \
        --password admin123
    
    # Инициализация с пропуском проблемных разрешений
    superset init --skip-perms
    
    touch ~/.superset_init_complete
fi

# Запуск сервера
exec gunicorn \
    --bind "0.0.0.0:8088" \
    --workers 1 \
    --threads 4 \
    --timeout 120 \
    "superset.app:create_app()"