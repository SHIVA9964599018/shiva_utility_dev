import oracledb as cx_Oracle
cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\shivac\Downloads\instantclient-basic-windows.x64-23.8.0.25.04\instantclient_23_8")

import pandas as pd
import json
import teradatasql
from google.cloud import bigquery
from utils import *
from chat_ui import *
import numpy as np

INDENT = " " * 10


def clean_number(x):
    try:
        val = float(x)
        if np.isclose(val, round(val), atol=1e-6):
            return int(round(val))
        else:
            return round(val, 4)
    except Exception:
        return x

def print_parity_validation_data():
    """
    1. Ask user for workflow (1=FI, 2=RI).
    2. Prompt for table names.
    3. Query BQ for latest run of those tables.
    4. Filter in Python for workflow (gcp* = RI, else FI).
    5. Show filtered tables with serial numbers. Ask for last 10 runs.
    6. If yes, prompt for serial number(s), query, and display last 10 runs.
    """

    # 1. Ask workflow FIRST
    print("Select workflow type:")
    print("1) FI (TD/ORACLE → BQ)")
    print("2) RI (BQ → TD)")
    wf_choice = input("Enter choice (1 or 2): ").strip()
    workflow_type_input = {"1": "FI", "2": "RI"}.get(wf_choice)
    if not workflow_type_input:
        print("[ERROR] Invalid workflow type selection. Exiting.")
        return

    # 2. Ask for table names
    print("[INFO] Please enter TARGET_TABLE_NAME values (comma-separated or line-separated):")
    input_tables = input("Enter target_table_names (comma or newline separated):\n").strip()
    table_names = [tbl.strip().upper() for tbl in input_tables.replace(",", "\n").splitlines() if tbl.strip()]
    print(f"[INFO] Parsed table names: {table_names}")

    if not table_names:
        print("[ERROR] No tables provided. Exiting.")
        return

    print("[INFO] Connecting to BigQuery  ...")
    client = bigquery.Client.from_service_account_json("BQ_PRD_Key.json")
    print("[INFO] Connected to BigQuery.")

    formatted_tables = ', '.join(f"'{name}'" for name in table_names)
    print("[INFO] Fetching latest run for all provided tables ...")
    nth_iteration = input(
        "Would you like to view a specific iteration?"
        "Press 1 for the default (latest) iteration,"
        "Press 2 for the previous (n-1) iteration"
    ).strip()

    if not nth_iteration.isdigit():
        print("Invalid input so default making it as 1 (i.e. fetches all the latest runs)")
        nth_iteration = 1
    else:
        nth_iteration = int(nth_iteration)

    query_latest = f"""
    SELECT SOURCE_DB_NAME, SOURCE_SCHEMA, TARGET_TABLE_NAME, SOURCE_TOTAL_COUNT, TARGET_TOTAL_COUNT,
           SOURCE_TOTAL_COUNT - TARGET_TOTAL_COUNT AS diff,
           LAST_JOB_EXECUTION_DATE, ROW_COUNT_DIFF, PARITY_CHECK_DATETIME,DATETIME_ADD(DATETIME_ADD(PARITY_CHECK_DATETIME, INTERVAL 12 HOUR), INTERVAL 30 MINUTE) AS PARITY_CHECK_DATETIME_IST,
           SOURCE_PARALLEL_COUNT, EXTRACT_TYPE_VALUE, SOURCE_DELETED_FLAG
    FROM (
        SELECT a.*,
               ROW_NUMBER() OVER (PARTITION BY table_id ORDER BY PARITY_CHECK_DATETIME DESC) rnk
        FROM `gcp-edwprddata-prd-33200.SSDB.PARITY_VALIDATION_DATA` a
        WHERE TARGET_TABLE_NAME IN ({formatted_tables})
    ) X
    WHERE rnk = {nth_iteration}
    ORDER BY TARGET_TABLE_NAME, PARITY_CHECK_DATETIME DESC
    """

    df_latest = client.query(query_latest).to_dataframe()
    print(f"[INFO] Query executed. Rows fetched: {len(df_latest)}")

    # --- Clean up numeric columns ---
    int_columns = ['SOURCE_TOTAL_COUNT', 'TARGET_TOTAL_COUNT', 'SOURCE_PARALLEL_COUNT']
    num_columns = ['diff', 'ROW_COUNT_DIFF']
    for col in int_columns:
        if col in df_latest.columns:
            try:
                df_latest[col] = df_latest[col].astype('Int64')
            except Exception:
                pass
    for col in num_columns:
        if col in df_latest.columns:
            df_latest[col] = df_latest[col].apply(clean_number)

    # 4. Filter by workflow type
    if workflow_type_input == 'RI':
        filtered_df = df_latest[df_latest['SOURCE_DB_NAME'].str.lower().str.startswith('gcp')]
    else:
        filtered_df = df_latest[~df_latest['SOURCE_DB_NAME'].str.lower().str.startswith('gcp')]

    if filtered_df.empty:
        print(f"\n[INFO] No tables found for workflow '{workflow_type_input}'.")
        return

    # Assign serial numbers for easy selection
    filtered_df = filtered_df.reset_index(drop=True)
    filtered_df['SERIAL_NO'] = filtered_df.index + 1

    # Display with serial numbers
    print(f"\n=== Latest Parity Validation Data for {workflow_type_input} ===")
    display_cols = ['SERIAL_NO'] + [col for col in filtered_df.columns if col != 'SERIAL_NO']
    print(insert_table_black(filtered_df[display_cols]))

    # 6. Ask for last 10 runs
    while True:
        answer = input("\nDo you want to display the last N runs data for any table? (y/n): ").strip().lower()

        if answer == 'y':
            # --- Your existing logic to show last 10 runs ---
            print("Select one or more tables for last N runs by serial number (comma or space separated):")
            for idx, row in filtered_df.iterrows():
                print(f"{row['SERIAL_NO']}) {row['TARGET_TABLE_NAME']}")

            sel_serials = input("Enter serial number(s) for last N runs:\n").replace(',', ' ').split()
            selected_indices = set()
            for s in sel_serials:
                try:
                    selected_indices.add(int(s))
                except Exception:
                    pass

            selected_rows = filtered_df[filtered_df['SERIAL_NO'].isin(selected_indices)]

            if selected_rows.empty:
                print("[INFO] No valid tables selected for last N runs. Done.")
                continue  # Go back to prompt

            for _, row in selected_rows.iterrows():
                table_name = row['TARGET_TABLE_NAME']
                source_db_name = row['SOURCE_DB_NAME']
                if workflow_type_input == 'RI':
                    workflow_filter = "LOWER(SOURCE_DB_NAME) LIKE 'gcp%'"
                else:
                    workflow_filter = "LOWER(SOURCE_DB_NAME) NOT LIKE 'gcp%'"
                user_input = input("Enter number of last N Runs to be displayed : ").strip()
                if not user_input.isdigit():
                    print("Invalid input!")
                    break  # exits your existing while loop
                no_of_iterations = int(user_input)
                print(f"[INFO] Fetching last {no_of_iterations} runs for table: {table_name} (Workflow: {workflow_type_input})")

                query_last10 = f"""
                SELECT SOURCE_DB_NAME, SOURCE_SCHEMA, TARGET_TABLE_NAME, SOURCE_TOTAL_COUNT, TARGET_TOTAL_COUNT,
                       SOURCE_TOTAL_COUNT - TARGET_TOTAL_COUNT AS diff,
                       LAST_JOB_EXECUTION_DATE, ROW_COUNT_DIFF, PARITY_CHECK_DATETIME,DATETIME_ADD(DATETIME_ADD(PARITY_CHECK_DATETIME, INTERVAL 12 HOUR), INTERVAL 30 MINUTE) AS PARITY_CHECK_DATETIME_IST,
                       SOURCE_PARALLEL_COUNT, EXTRACT_TYPE_VALUE, SOURCE_DELETED_FLAG
                FROM (
                    SELECT a.*,
                           ROW_NUMBER() OVER (PARTITION BY table_id ORDER BY PARITY_CHECK_DATETIME DESC) rnk
                    FROM `gcp-edwprddata-prd-33200.SSDB.PARITY_VALIDATION_DATA` a
                    WHERE TARGET_TABLE_NAME = '{table_name}'
                          AND {workflow_filter}
                ) X
                WHERE rnk <= {no_of_iterations}
                ORDER BY PARITY_CHECK_DATETIME DESC
                """
                df_last10 = client.query(query_last10).to_dataframe()

                for col in int_columns:
                    if col in df_last10.columns:
                        try:
                            df_last10[col] = df_last10[col].astype('Int64')
                        except Exception:
                            pass
                for col in num_columns:
                    if col in df_last10.columns:
                        df_last10[col] = df_last10[col].apply(clean_number)

                print(f"=== Last N Parity Validation Runs for {table_name} (Workflow: {workflow_type_input}) ===\n")
                df_last10.insert(0,"SNO",range(1,len(df_last10)+1))
                insert_table_black(df_last10)

            print(f"\n[INFO] Done displaying last {no_of_iterations} runs.\n")
            # Loop continues asking again

        elif answer == 'n':
            print(f"[INFO] Exiting last N runs display.")
            break  # Exit the loop and function

        else:
            print("[INFO] Invalid input. Please enter 'y' or 'n'.")






