#!/bin/bash

set -e

# Инициализация только при первом запуске
if [ ! -f ~/.superset_initialized ]; then
    # Генерация SECRET_KEY
    export SECRET_KEY=$(python -c "import secrets; print(secrets.token_urlsafe(64))")
    echo $SECRET_KEY > ~/.superset_secret_key
    
    # Инициализация БД
    superset db upgrade
    
    # Создание администратора (с проверкой существования)
    if ! superset fab list-users | grep -q admin; then
        superset fab create-admin \
            --username admin \
            --firstname Admin \
            --lastname User \
            --email admin@example.com \
            --password admin123
    fi
    
    # Инициализация с фиксом для новых версий
    superset init
    
    # Установка Pillow если нужно
    pip install pillow || echo "Pillow installation failed, continuing without it"
    
    touch ~/.superset_initialized
fi

# Загрузка SECRET_KEY при последующих запусках
export SECRET_KEY=$(cat ~/.superset_secret_key)

# Запуск сервера
exec gunicorn \
    --bind "0.0.0.0:8088" \
    --workers 1 \
    --threads 4 \
    --timeout 120 \
    "superset.app:create_app()"