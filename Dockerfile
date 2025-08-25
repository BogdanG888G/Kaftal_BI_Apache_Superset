FROM apache/superset:latest

USER root

# Установка системных зависимостей
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    freetds-dev \
    freetds-bin \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Установка pip в виртуальное окружение
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | /app/.venv/bin/python

# Установка драйверов через установленный pip
RUN /app/.venv/bin/pip install --no-cache-dir pymssql pyodbc

COPY requirements.txt /app/
RUN if [ -f /app/requirements.txt ]; then \
    /app/.venv/bin/pip install --no-cache-dir -r /app/requirements.txt; \
    fi && \
    rm -f /app/requirements.txt

COPY mssql_to_ch.py /app/
RUN /app/.venv/bin/pip install --no-cache-dir tqdm pandas \
    clickhouse-driver==0.2.4 clickhouse-sqlalchemy==0.2.4

COPY superset_config.py /app/pythonpath/
COPY docker-init.sh /app/

RUN chown -R superset:superset /app && \
    chmod +x /app/docker-init.sh

USER superset
ENTRYPOINT ["/app/docker-init.sh"]