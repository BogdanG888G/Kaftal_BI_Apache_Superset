# 🚀 BI-платформа компании "Кафталь" на Superset + ClickHouse + MSSQL


![Superset](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![SQL Server](https://img.shields.io/badge/Microsoft%20SQL%20Server-CC2927?style=for-the-badge&logo=microsoft%20sql%20server&logoColor=white)

Этот проект — полноценная **BI-среда в Docker**, где данные из MSSQL автоматически переливаются в ClickHouse, после чего визуализируются в Apache Superset. Всё разворачивается с нуля одной командой через `docker-compose up`.  

> 🧩 Используется пайплайн: **MSSQL → Python data-transfer → ClickHouse → Apache Superset**

---

## 📦 Стек технологий

| Компонент        | Назначение                                  |
|------------------|----------------------------------------------|
| **Apache Superset** | визуализация данных, дашборды               |
| **ClickHouse DB**   | аналитическая СУБД (colum-oriented)         |
| **MSSQL Server**    | источник данных для выгрузки                |
| **Python + pyodbc** | перенос данных из MSSQL в ClickHouse        |
| **Docker Compose**  | orchestration всех сервисов                 |
| **SQLAlchemy / clickhouse-driver** | подключение ClickHouse к Superset |

---

## 🛠 Структура проекта

```

.
├── docker-compose.yml
├── Dockerfile      <-- Кастомный образ Superset с ODBC и ClickHouse драйверами
├── requirements.txt
├── mssql\_to\_ch.py  <-- скрипт перелива MSSQL → ClickHouse
├── superset\_config.py
├── docker-init.sh  <-- первичный старт и инициализация Superset
└── /data, /superset\_data, /clickhouse\_data  <-- persist volume

````

---

## ✅ Быстрый запуск

```bash
docker compose build superset      # 1 раз
docker compose up -d               # запускаем всё
````

Через 30–40 секунд Superset будет доступен по адресу:
<br>
[http://localhost:8088](http://localhost:8088) 
<br>
<br>
login: admin 
<br>
password admin123

---

## 🔐 Доступы / креды

| Сервис          | URL / порт                                     | Логин / пароль    |
| --------------- | ---------------------------------------------- | ----------------- |
| Superset        | [http://localhost:8088](http://localhost:8088) | `admin` / `admin` |
| ClickHouse HTTP | [http://localhost:8123](http://localhost:8123) | `admin` / `123`   |
| ClickHouse DB   | native port 9000                               | `admin` / `123`   |

---

## 🔗 Пример строки подключения ClickHouse в Superset

HTTP драйвер:

```
clickhouse+http://admin:123@clickhouse:8123/default
```

Native драйвер:

```
clickhouse+native://admin:123@clickhouse:9000/default
```

> Если Superset и ClickHouse в одном `docker-compose`, то hostname — `clickhouse`

---

## ⚙️ Добавление Data Source в Superset (вручную)

1. Открыть Superset → *Data → Databases → + Database*
2. Вставить SQLAlchemy URI (пример выше)
3. Нажать **Test connection** → Save
4. Перейти во вкладку *Datasets* → Add → выбрать нужную таблицу ClickHouse
5. Теперь можно строить чарты, explore view и дашборды

---

## 🔄 Передача данных: MSSQL → ClickHouse

🧩 Скрипт `mssql_to_ch.py` автоматически:

* Подключается к MSSQL через pyodbc
* Читает таблицу порциями (batch size задаётся переменной)
* Загружает через `INSERT INTO ...` в ClickHouse
* Используется в сервисе `data-transfer` внутри docker-compose

Пример переменных окружения задаётся внутри контейнера:

```yaml
environment:
  - MSSQL_SERVER=host.docker.internal
  - MSSQL_PORT=1433
  - MSSQL_DATABASE=Stage
  - MSSQL_USER=superset_user
  - MSSQL_PASSWORD=123
  - CH_HOST=clickhouse
  - CH_PORT=9000
  - CH_USER=admin
  - CH_PASSWORD=123
  - TABLE_TO_TRANSFER=bi.ALL_DATA_COMPETITORS_MATERIALIZED
  - BATCH_SIZE=10000
```

---

## 📊 (TODO) Снимок готового дашборда

![dashboard placeholder](./screenshots/dashboard.png)

> Здесь будет красивый скриншот, как только всё визуализируем 😎

---

## Крутые особенности

* ✅ Автоматическое добавление ClickHouse в Superset через API
* ✅ Автоматическое обновление данных в ClickHouse (MSSQL → CH → Superset refresh)
* ✅ Dockerfile полностью воспроизводимый


---

<p align="center">
  <img src="https://img.shields.io/badge/python-3.10-blue">
  <img src="https://img.shields.io/badge/superset-3.x-yellow">
  <img src="https://img.shields.io/badge/clickhouse-22+-orange">
  <img src="https://img.shields.io/badge/Database-MSSQL-blueviolet">
</p>