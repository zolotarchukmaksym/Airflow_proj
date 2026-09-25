from datetime import datetime
import os
import pandas as pd
from sqlalchemy import text

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

RAW_FILE_PATH = "/opt/airflow/data/raw/Inventory_management.xlsx"
PROCESSED_DIR = "/opt/airflow/data/processed"


def check_raw_file():
    if not os.path.exists(RAW_FILE_PATH):
        raise FileNotFoundError(f"Raw data file not found: {RAW_FILE_PATH}")
    print(f"Raw data file exists: {RAW_FILE_PATH}")


def load_raw_data(**context):
    required_sheets = [
        "Products",
        "Inventory_Beginning",
        "Inventory_UnitsIn",
        "Inventory_UnitsOut",
        "Inventory_Calendar",
    ]

    excel_file = pd.ExcelFile(RAW_FILE_PATH, engine="openpyxl")
    available_sheets = excel_file.sheet_names

    missing_sheets = [sheet for sheet in required_sheets if sheet not in available_sheets]
    if missing_sheets:
        raise ValueError(f"Missing required sheets: {missing_sheets}")

    sheet_paths = {}
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    for sheet_name in required_sheets:
        df = pd.read_excel(RAW_FILE_PATH, sheet_name=sheet_name, engine="openpyxl")
        temp_path = os.path.join(PROCESSED_DIR, f"_staging_{sheet_name}.csv")
        df.to_csv(temp_path, index=False)
        sheet_paths[sheet_name] = temp_path
        print(f"Loaded sheet: {sheet_name}, rows={len(df)}")

    context["ti"].xcom_push(key="sheet_paths", value=sheet_paths)


def aggregate_and_build_dw_datasets(**context):

    sheet_paths = context["ti"].xcom_pull(key="sheet_paths", task_ids="load_raw_data")

    products = pd.read_csv(sheet_paths["Products"])
    calendar = pd.read_csv(sheet_paths["Inventory_Calendar"])
    inventory_beginning = pd.read_csv(sheet_paths["Inventory_Beginning"])
    units_in = pd.read_csv(sheet_paths["Inventory_UnitsIn"])
    units_out = pd.read_csv(sheet_paths["Inventory_UnitsOut"])

    dim_product = products.drop_duplicates(subset=["ProductAlternateKey"]).copy()
    dim_product['ProductKey'] = range(1, len(dim_product) + 1)

    prod_cols_to_keep = [
        'ProductKey', 'ProductAlternateKey', 'EnglishProductName',
        'StandardCost', 'Color', 'ListPrice', 'ModelName'
    ]
    existing_prod_cols = [col for col in prod_cols_to_keep if col in dim_product.columns]
    dim_product = dim_product[existing_prod_cols]
    dim_product.to_csv(os.path.join(PROCESSED_DIR, "dim_product.csv"), index=False)

    dim_date = calendar.drop_duplicates(subset=["DateKey"]).copy()

    date_cols_to_keep = [
        'DateKey', 'FullDateAlternateKey', 'DayNumberOfWeek', 'EnglishDayNameOfWeek',
        'DayNumberOfMonth', 'MonthNumberOfYear', 'EnglishMonthName',
        'CalendarQuarter', 'CalendarYear', 'FiscalYear'
    ]
    existing_date_cols = [col for col in date_cols_to_keep if col in dim_date.columns]
    dim_date = dim_date[existing_date_cols]
    dim_date.to_csv(os.path.join(PROCESSED_DIR, "dim_date.csv"), index=False)

    prod_map = dict(zip(products['ProductAlternateKey'], dim_product['ProductKey']))

    df_in = units_in.copy()
    df_in['ProductKey'] = df_in['ProductAlternateKey'].map(prod_map)
    df_in['CostIn'] = (df_in['UnitsIn'] * df_in['UnitCost']).round(2)
    agg_in = df_in.groupby(['DateKey', 'ProductKey'], as_index=False).agg({'UnitsIn': 'sum', 'CostIn': 'sum'})

    df_out = units_out.copy()
    df_out['ProductKey'] = df_out['ProductAlternateKey'].map(prod_map)
    df_out['TotalOut'] = (df_out['UnitsOut'] * df_out['UnitCost']).round(2)
    agg_out = df_out.groupby(['DateKey', 'ProductKey'], as_index=False).agg({'UnitsOut': 'sum', 'TotalOut': 'sum'})

    df_bal = inventory_beginning.copy()
    df_bal['ProductKey'] = df_bal['ProductAlternateKey'].map(prod_map)
    agg_bal = df_bal.groupby(['DateKey', 'ProductKey'], as_index=False).agg({'UnitsBalance': 'sum'})

    fact_inventory = pd.merge(agg_bal, agg_in, on=['DateKey', 'ProductKey'], how='outer')
    fact_inventory = pd.merge(fact_inventory, agg_out, on=['DateKey', 'ProductKey'], how='outer')

    fact_inventory.fillna(0, inplace=True)

    final_fact_columns = [
        'DateKey', 'ProductKey', 'UnitsBalance', 'UnitsIn', 'CostIn', 'UnitsOut', 'TotalOut'
    ]

    existing_fact_cols = [col for col in final_fact_columns if col in fact_inventory.columns]
    fact_inventory = fact_inventory[existing_fact_cols]

    fact_inventory.to_csv(os.path.join(PROCESSED_DIR, "fact_inventory.csv"), index=False)
    print(f"Aggregated fact_inventory: {len(fact_inventory)} rows")


def load_data_to_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    dim_product = pd.read_csv(os.path.join(PROCESSED_DIR, "dim_product.csv"))
    dim_date = pd.read_csv(os.path.join(PROCESSED_DIR, "dim_date.csv"))
    fact_inventory = pd.read_csv(os.path.join(PROCESSED_DIR, "fact_inventory.csv"))

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS fact_inventory CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS dim_product CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS dim_date CASCADE;"))

        dim_date.to_sql('dim_date', conn, if_exists='replace', index=False)
        dim_product.to_sql('dim_product', conn, if_exists='replace', index=False)
        fact_inventory.to_sql('fact_inventory', conn, if_exists='replace', index=False)

    print("Чисті дані успішно завантажені у сховище PostgreSQL!")

with DAG(
        dag_id="inventory_dw_pipeline",
        start_date=datetime(2025, 1, 1),
        schedule="@once",
        catchup=False,
        tags=["lab", "data_warehouse", "etl"],
        description="Data pipeline for loading, aggregating and saving inventory data to DW",
) as dag:
    task_check_raw_file = PythonOperator(
        task_id="check_raw_file",
        python_callable=check_raw_file,
    )

    task_load_raw_data = PythonOperator(
        task_id="load_raw_data",
        python_callable=load_raw_data,
    )

    task_aggregate_data = PythonOperator(
        task_id="aggregate_and_build_dw_datasets",
        python_callable=aggregate_and_build_dw_datasets,
    )

    task_init_db_logs = SQLExecuteQueryOperator(
        task_id="init_db_logs",
        conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS etl_pipeline_logs (
                log_id SERIAL PRIMARY KEY,
                run_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(100)
            );
            INSERT INTO etl_pipeline_logs (status) VALUES ('Inventory DW Pipeline Started');
        """
    )

    task_load_to_db = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=load_data_to_postgres,
    )

    task_check_raw_file >> task_load_raw_data >> task_aggregate_data >> task_init_db_logs >> task_load_to_db
