# dags/realty_merge_dag.py

import os
import pendulum
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import Table, Column, Integer, Float, Boolean, MetaData, UniqueConstraint
from messages import send_telegram_success_message, send_telegram_failure_message

load_dotenv()

@dag(
    dag_id="realty_merge_dag",
    schedule="@once",
    start_date=pendulum.datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    tags=["realty", "merge", "etl"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
)
def merge_realty_data():

    @task()
    def create_table():
        """Создание результирующей таблицы, если не существует"""
        hook = PostgresHook(postgres_conn_id="destination_db")
        engine = hook.get_sqlalchemy_engine()
        metadata = MetaData()

        merged = Table(
            "merged_realty_data",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("floor", Integer),
            Column("is_apartment", Boolean),
            Column("kitchen_area", Float),
            Column("living_area", Float),
            Column("rooms", Integer),
            Column("studio", Boolean),
            Column("total_area", Float),
            Column("price", Float),
            Column("unit_price", Float),
            Column("building_id", Integer),
            Column("build_year", Integer),
            Column("building_type_int", Integer),
            Column("latitude", Float),
            Column("longitude", Float),
            Column("ceiling_height", Float),
            Column("flats_count", Integer),
            Column("floors_total", Integer),
            Column("has_elevator", Boolean),
            UniqueConstraint("id", name="uniq_flat_id")
        )

        if not sqlalchemy.inspect(engine).has_table("merged_realty_data"):
            metadata.create_all(engine)
            print("Создана таблица 'merged_realty_data'.")
        else:
            print("Таблица 'merged_realty_data' уже существует.")

    @task()
    def extract_data():
        """Извлечение и объединение данных из flats и buildings"""
        hook = PostgresHook(postgres_conn_id="destination_db")
        conn = hook.get_conn()

        query = """
            SELECT 
                f.id,
                f.floor,
                f.is_apartment,
                f.kitchen_area,
                f.living_area,
                f.rooms,
                f.studio,
                f.total_area,
                f.price,
                b.id AS building_id,
                b.build_year,
                b.building_type_int,
                b.latitude,
                b.longitude,
                b.ceiling_height,
                b.flats_count,
                b.floors_total,
                b.has_elevator
            FROM flats f
            JOIN buildings b ON f.building_id = b.id
        """

        df = pd.read_sql(query, conn)
        print(f"Извлечено строк: {len(df)}")
        return df

    @task()
    def transform_data(df: pd.DataFrame) -> pd.DataFrame:
        """Добавление новых признаков и обработка данных"""
        df["unit_price"] = df["price"] / df["total_area"]
        df = df.drop_duplicates(subset=["id"])
        return df

    @task()
    def load_data(df: pd.DataFrame):
        """Загрузка объединённых данных в таблицу"""
        hook = PostgresHook(postgres_conn_id="destination_db")

        rows = df.to_dict(orient="records")

        hook.insert_rows(
            table="merged_realty_data",
            rows=[tuple(row.values()) for row in rows],
            target_fields=list(df.columns),
            replace=False,
            on_conflict_do_update={
                "target": "uniq_flat_id",
                "action": "UPDATE",
                "conflict_column": "id"
            }
        )
        print(f"Загружено строк: {len(rows)}")

    # Pipeline
    create = create_table()
    raw_df = extract_data()
    processed_df = transform_data(raw_df)
    load = load_data(processed_df)

    create >> raw_df >> processed_df >> load

merge_realty_data_dag = merge_realty_data()