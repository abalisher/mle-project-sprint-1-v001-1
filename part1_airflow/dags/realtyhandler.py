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

def remove_duplicates(data):
    feature_cols = data.columns.drop('id').tolist()
    is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
    data = data[~is_duplicated_features].reset_index(drop=True)
    return data 

def fill_missing_values(data):

    cols_with_nans = data.isnull().sum()
    cols_with_nans = cols_with_nans[cols_with_nans > 0].index

    for col in cols_with_nans:

        if data[col].dtype in [float, int]:
            fill_value = data[col].mean()
        elif data[col].dtype == 'object':
            fill_value = data[col].mode().iloc[0]

        data[col] = data[col].fillna(fill_value)

    return data

def remove_outliers_iqr(data, threshold=1.5):    
    numeric_cols = data.select_dtypes(include=['float64', 'int64']).columns
    mask = pd.Series([True] * len(data))

    for col in numeric_cols:        
        Q1 = data[col].quantile(0.25)
        Q3 = data[col].quantile(0.75)
        IQR = Q3 - Q1
        margin = threshold * IQR
        lower = Q1 - margin
        upper = Q3 + margin
        col_mask  = data[col].between(lower, upper)
        
        mask &= col_mask 


    cleaned_data = data[mask].reset_index(drop=True)
    print(f"\nИтог: удалено {len(data) - len(cleaned_data)} строк в сумме")
    return cleaned_data

@dag(
    dag_id="realty_clean_dag",
    schedule="@once",
    start_date=pendulum.datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    tags=["realty", "clean", "etl"],
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
            "cleaned_realty_data",
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
            UniqueConstraint("id", name="c_uniq_flat_id")
        )

        if not sqlalchemy.inspect(engine).has_table("cleaned_realty_data"):
            metadata.create_all(engine)
            print("Создана таблица 'cleaned_realty_data'.")
        else:
            print("Таблица 'cleaned_realty_data' уже существует.")

    @task()
    def extract_data():
        """Извлечение и объединение данных из flats и buildings"""
        hook = PostgresHook('destination_db')
        engine = hook.get_sqlalchemy_engine()
        df = pd.read_sql_table('merged_realty_data', con=engine)
        print(f"Извлечено строк: {len(df)}")
        return df

    @task()
    def transform_data(df: pd.DataFrame) -> pd.DataFrame:
        df = remove_duplicates(df)        
        df = fill_missing_values(df)        
        df = remove_outliers_iqr(df)
        
        return df

    @task()
    def load_data(df: pd.DataFrame):
        hook = PostgresHook('destination_db')
        df.to_sql(
            'cleaned_realty_data',
            con=hook.get_sqlalchemy_engine(),
            if_exists='replace',
            index=False
        )

    # Pipeline
    create = create_table()
    raw_df = extract_data()
    processed_df = transform_data(raw_df)
    load = load_data(processed_df)

    create >> raw_df >> processed_df >> load

merge_realty_data_dag = merge_realty_data()