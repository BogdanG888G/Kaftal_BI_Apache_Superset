FROM apache/superset:latest

USER root

# Установка системных зависимостей
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    freetds-dev \
    freetds-bin \
    unixodbc-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Установка дополнительных Python-пакетов через pip
RUN pip install --no-cache-dir \
    pymssql \
    pyodbc \
    tqdm \
    pandas \
    clickhouse-driver==0.2.4 \
    clickhouse-sqlalchemy==0.2.4

# Копирование файлов
COPY requirements.txt /app/
RUN if [ -f /app/requirements.txt ]; then \
    pip install --no-cache-dir -r /app/requirements.txt; \
    fi && \
    rm -f /app/requirements.txt

COPY mssql_to_ch.py /app/
COPY superset_config.py /app/pythonpath/
COPY docker-init.sh /app/

RUN chown -R superset:superset /app && \
    chmod +x /app/docker-init.sh

USER superset
ENTRYPOINT ["/app/docker-init.sh"]