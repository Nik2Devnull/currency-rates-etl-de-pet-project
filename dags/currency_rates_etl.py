"""
ETL-пайплайн: курсы валют ЦБ РФ -> Postgres DWH.

Схема пайплайна:
    extract_rates -> load_to_staging -> load_to_core -> dq_checks -> build_mart

Граф - линейный.

Слои данных:
    stg  — получение raw JSON от API;
    core — очищенные типизированные данные;
    mart — витрина для аналитиков.
"""

import logging
from pathlib import Path
import json
import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

CBR_ARCHIVE_URL = "https://www.cbr-xml-daily.ru/archive/{year}/{month:02d}/{day:02d}/daily_json.js"
DWH_CONN_ID = "dwh_postgres"
SQL_DIR = Path("/opt/airflow/sql/etl")

def _read_sql(filename: str) -> str:
    return (SQL_DIR / filename).read_text(encoding="utf-8")

@dag(
    dag_id="currency_rates_etl",
    description="Ежедневная загрузка курсов валют ЦБ РФ в DWH",
    schedule="0 12 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["pet-project", "cbr", "etl"],
)
def currency_rates_etl():

    @task
    def extract_rates(ds: str | None = None) -> dict:
        date = pendulum.parse(ds)
        url = CBR_ARCHIVE_URL.format(year=date.year, month=date.month, day=date.day)
        logger.info("Запрос курсов валют: %s", url)

        response = requests.get(url, timeout=30)
        if response.status_code == 404:
            raise AirflowSkipException(f"Нет данных на дату: {ds}")
        response.raise_for_status()

        payload = response.json()
        n_currencies = len(payload.get("Valute", {}))
        logger.info("Получено валют: %d", n_currencies)
        if n_currencies == 0:
            raise ValueError("API вернул пустой список валют")
        return payload

    @task
    def load_to_staging(payload: dict, ds: str | None = None) -> None:
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        hook.run(
            [
                "DELETE FROM stg.currency_rates_raw WHERE rate_date = %(ds)s",
                "INSERT INTO stg.currency_rates_raw (rate_date, payload) VALUES (%(ds)s, %(payload)s)",
            ],
            parameters={"ds": ds, "payload": json.dumps(payload)},
        )
        logger.info("Сырые данные за %s загружены в stg.currency_rates_raw", ds)

    @task
    def load_to_core(ds: str | None = None) -> None:
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        hook.run(_read_sql("load_core.sql"), parameters={"ds": ds})
        logger.info("Слой core за дату: %s обновлён", ds)

    @task
    def dq_checks(ds: str | None = None) -> None:
        """Data quality checks"""
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)

        checks = {
            "В core есть строки за дату": """
                SELECT COUNT(*) > 0
                FROM core.currency_rates WHERE rate_date = %(ds)s
            """,
            "Нет NULL в ключевых полях": """
                SELECT COUNT(*) = 0
                FROM core.currency_rates
                WHERE rate_date = %(ds)s AND (char_code IS NULL OR rate IS NULL OR nominal IS NULL)
            """,
            "Все курсы положительные": """
                SELECT COUNT(*) = 0
                FROM core.currency_rates
                WHERE rate_date = %(ds)s AND (rate <= 0 OR nominal <= 0)
            """,
            "Нет дублей по (дата, валюта)": """
                SELECT COUNT(*) = 0 
                FROM (
                    SELECT char_code
                    FROM core.currency_rates
                    WHERE rate_date = %(ds)s
                    GROUP BY char_code
                    HAVING COUNT(*) > 1
                ) dup
            """,
        }

        failed = []
        for name, sql in checks.items():
            ok = hook.get_first(sql, parameters={"ds": ds})[0]
            logger.info("DQ-проверка '%s': %s", name, "OK" if ok else "FAIL")
            if not ok:
                failed.append(name)

        if failed:
            raise ValueError(f"Провалены DQ-проверки: {failed}")

    @task
    def build_mart(ds: str | None = None) -> None:
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        hook.run(_read_sql("build_mart.sql"), parameters={"ds": ds})
        logger.info("Витрина mart.currency_rates_daily за дату: %s обновлена", ds)

    payload = extract_rates()
    load_to_staging(payload) >> load_to_core() >> dq_checks() >> build_mart()


currency_rates_etl()
