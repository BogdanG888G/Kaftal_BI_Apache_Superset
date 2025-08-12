#!/bin/bash

# Initialize Superset
superset db upgrade
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@admin.com \
    --password admin123
superset init

# Start server
exec superset run -p 8088 --with-threads --reload --debugger