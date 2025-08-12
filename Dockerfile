FROM apache/superset:latest

USER root

# 1. Установка системных зависимостей
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gnupg \
    curl \
    unixodbc \
    odbcinst \
    odbcinst1debian2 \
    libodbc1 \
    && rm -rf /var/lib/apt/lists/*

# 2. Установка драйвера MSSQL (новый метод)
RUN mkdir -p /etc/apt/keyrings && \
    curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg && \
    echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/11/prod bullseye main" > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    rm -rf /var/lib/apt/lists/*

# 3. Установка Python-зависимостей
RUN pip install --no-cache-dir \
    pyodbc==4.0.35 \
    pillow

USER superset

# Точка входа остается стандартной (из базового образа)