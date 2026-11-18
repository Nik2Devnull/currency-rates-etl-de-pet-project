# Currency Rates ETL — pet-проект Data Engineer

Ежедневный ETL-пайплайн на **Apache Airflow**: забирает курсы валют с API ЦБ РФ,  
складывает в **PostgreSQL** по слоям `stg → core → mart` и проверяет качество данных.

## Архитектура

```
API ЦБ РФ (JSON) -> stg.currency_rates_raw -> core.currency_rates -> mart.currency_rates_daily
```

- stg.currency_rates_raw - хранит сырые JSON
- core.currency_rates - хранит очищенные данные
- mart.currency_rates_daily - хранит готовые витрины для аналитиков

Шаги DAG (скрипт `./dags/currency_rates_etl.py`):

| Таска | Что делает |
|---|---|
| `extract_rates` | Запрос к API, retries |
| `load_to_staging` | Кладет сырой JSON в staging layer |
| `load_to_core` | Парсинг jsonb в типизированную таблицу|
| `dq_checks` | Набор тестов core data: Строки есть, нет NULL, курсы > 0, нет дублей — иначе пайплайн падает |
| `build_mart` | Витрина |

## Запуск

Скопируйте пример окружения и при необходимости поменяйте пароли. Файл `.env` в git не попадает.

```bash
cp .env.example .env
docker compose up airflow-init
docker compose up -d             # поднять webserver, scheduler и две БД
```

UI Airflow — http://localhost:8080 (логин и пароль `admin` / `admin`).

**Догрузить историю за месяц:**

```bash
docker compose exec airflow-scheduler \
  airflow dags backfill currency_rates_etl -s 2026-10-01 -e 2026-11-01
```

На этом примере загрузка данных с 1 октября 2026 по 1 ноября 2026.

## Посмотреть данные

DWH доступен снаружи: `localhost:5432`

```bash
docker compose exec postgres-dwh psql -U dwh -d dwh
```

В `.env.example` пользователь — `dwh`. Если в своём `.env` имя другое, подставьте значение `DWH_POSTGRES_USER`.

Примеры SQL запросов аналитиков:

```sql
-- Курс доллара и его динамика
SELECT rate_date, rate_per_unit, change_pct, avg_7d
FROM mart.currency_rates_daily
WHERE char_code = 'USD'
ORDER BY rate_date DESC;

-- Топ-5 валют по росту за последний день
SELECT char_code, rate_per_unit, change_pct
FROM mart.currency_rates_daily
WHERE rate_date = (SELECT max(rate_date) FROM mart.currency_rates_daily)
ORDER BY change_pct DESC NULLS LAST
LIMIT 5;
```

## Структура проекта

```
├── docker-compose.yml        Airflow (LocalExecutor) + 2 Postgres DB
├── .env.example              образец переменных, копируется в .env
├── dags/
│   └── currency_rates_etl.py DAG на TaskFlow API
└── sql/
    ├── init/01_schema.sql    схемы и таблицы DWH
    └── etl/
        ├── load_core.sql     stg -> core
        └── build_mart.sql    core -> mart
```
