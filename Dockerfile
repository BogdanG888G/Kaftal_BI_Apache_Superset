FROM apache/superset:latest

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gnupg2 ca-certificates curl g++ make && \
    rm -rf /var/lib/apt/lists/*

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt && \
    rm /app/requirements.txt

COPY mssql_to_ch.py /app/
RUN pip install tqdm pyodbc pandas \
    clickhouse-driver==0.2.4 clickhouse-sqlalchemy==0.2.4

COPY superset_config.py /app/pythonpath/
COPY docker-init.sh /app/

RUN chown -R superset:superset /app && chmod +x /app/docker-init.sh

USER superset
ENTRYPOINT ["/app/docker-init.sh"]
