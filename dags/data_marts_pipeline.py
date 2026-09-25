from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

SQL_DROP_DM_MONTHLY = "DROP TABLE IF EXISTS dm_monthly_product_movement;"

SQL_CREATE_DM_MONTHLY = """
    CREATE TABLE dm_monthly_product_movement AS
    SELECT 
        d."CalendarYear" AS year,
        d."MonthNumberOfYear" AS month,
        d."EnglishMonthName" AS month_name,
        p."EnglishProductName" AS product_name,
        p."Color" AS color,
        SUM(f."UnitsIn") AS total_units_in,
        SUM(f."UnitsOut") AS total_units_out,
        SUM(f."CostIn") AS total_cost_in,
        SUM(f."TotalOut") AS total_cost_out
    FROM fact_inventory f
    JOIN dim_date d ON f."DateKey" = d."DateKey"
    JOIN dim_product p ON f."ProductKey" = p."ProductKey"
    GROUP BY 1, 2, 3, 4, 5;
"""

SQL_DROP_DM_QUARTERLY = "DROP TABLE IF EXISTS dm_quarterly_financial_summary;"

SQL_CREATE_DM_QUARTERLY = """
    CREATE TABLE dm_quarterly_financial_summary AS
    SELECT 
        d."CalendarYear" AS year,
        d."CalendarQuarter" AS quarter,
        p."ModelName" AS model_name,
        SUM(f."CostIn") AS quarterly_cost_in,
        SUM(f."TotalOut") AS quarterly_revenue_out,
        SUM(f."TotalOut") - SUM(f."CostIn") AS quarterly_profit_margin
    FROM fact_inventory f
    JOIN dim_date d ON f."DateKey" = d."DateKey"
    JOIN dim_product p ON f."ProductKey" = p."ProductKey"
    GROUP BY 1, 2, 3;
"""

with DAG(
    dag_id="data_marts_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["lab4", "data_marts", "analytics"],
    description="Побудова вітрин даних для аналітики (Лабораторна 4)",
) as dag:

    task_dm_monthly = SQLExecuteQueryOperator(
        task_id="create_dm_monthly_product_movement",
        conn_id="postgres_default",
        sql=[SQL_DROP_DM_MONTHLY, SQL_CREATE_DM_MONTHLY]
    )

    task_dm_quarterly = SQLExecuteQueryOperator(
        task_id="create_dm_quarterly_financial_summary",
        conn_id="postgres_default",
        sql=[SQL_DROP_DM_QUARTERLY, SQL_CREATE_DM_QUARTERLY]
    )

    [task_dm_monthly, task_dm_quarterly]
