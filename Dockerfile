FROM apache/superset:latest

USER root

# 1. Установка системных зависимостей (без ODBC)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gnupg2 \
    ca-certificates \
    curl \
    g++ \
    make \
    && rm -rf /var/lib/apt/lists/*

# 2. Установка MSSQL ODBC драйвера (новый метод)
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc && \
    rm -rf /var/lib/apt/lists/*

# 3. Установка Python-зависимостей
COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt && \
    rm /app/requirements.txt

# В существующий Dockerfile добавьте:
COPY mssql_to_ch.py /app/
RUN pip install clickhouse-driver
RUN pip install tqdm pyodbc clickhouse-driver pandas

# 4. Копирование конфигурации
COPY superset_config.py /app/pythonpath/
COPY docker-init.sh /app/

# 5. Настройка прав
RUN chown -R superset:superset /app && \
    chmod +x /app/docker-init.sh

USER superset

ENTRYPOINT ["/app/docker-init.sh"]