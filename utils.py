import oracledb as cx_Oracle
cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\shivac\Downloads\instantclient-basic-windows.x64-23.8.0.25.04\instantclient_23_8")

import pandas as pd
import json
import teradatasql
from google.cloud import bigquery
from chat_ui import *
import os
from dotenv import load_dotenv
from datetime import datetime
import Job_promotion_automation
from sqlalchemy import create_engine
import decimal
import numpy as np
from datetime import timedelta
import config
# default: keep logs off if you set False here; you can toggle in config.py
# config.CONSOLE_PRINT = False
# Load .env file from parent directory (adjust path as discussed earlier)
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path=env_path)

INDENT = " " * 10

def log_message(message):
    """
    Global logger.
    Prints only when CONSOLE_PRINT is True.
    """
    import config
    if config.CONSOLE_PRINT:
        # use the real print to always print even when print was monkeypatched
        __builtins__['print'](message)

# --------------------
# Helper: keep raw print for user-facing messages if needed
# (We will not monkeypatch; replacements are explicit)
# --------------------

def currently_running_jobs(env,db_key):
    creds=load_db_credentials()
    try:
        with get_dynamic_db_connection(db_key, creds) as conn:
            with conn.cursor() as cur:
               while True:
                qry=f"""SELECT * FROM STM_MST WHERE RUN_STATUS in ('R','A') AND CURRENT_PHASE<>'Success' AND START_TIME >(SYSDATE -1) order by reqid desc"""
                log_message(f"Query->{qry}")
                df=query_to_dataframe(cur,qry)
                df.insert(0, "S.No", range(1, len(df)+1))
                insert_table_black(df)
                if input("Would you like to Refresh the Data [y] Yes").strip().lower()=="y":
                    continue
                else:
                    break
    except Exception as e:
        log_message(f"[ERROR] {e}")
        return None

def recently_failed_jobs(env, db_key):
    creds=load_db_credentials()
    try:
        with get_dynamic_db_connection(db_key, creds) as conn:
            with conn.cursor() as cur:
               while True:
                diff_filter=input("Enter how long back you would like to check the failed jobs for 1 day enter 1d, for 1 hour 1h").lower().strip()
                last_char = diff_filter[-1]
                except_last = diff_filter[:-1]
                if last_char=="h":
                    time_filter=f" AND START_TIME >( SYSDATE - {except_last}/24) "
                elif   last_char=="d":
                    time_filter = f" AND START_TIME >( SYSDATE - {except_last}) "
                else:
                    time_filter = " AND START_TIME >( SYSDATE - 1) "

                qry=f"""SELECT * FROM STM_MST WHERE CURRENT_PHASE<>'Success' {time_filter}
                        AND RUN_STATUS<>'R' AND IST_END_TIME is not null order by reqid desc"""
                log_message(f"Query->{qry}")
                df=query_to_dataframe(cur,qry)
                df.insert(0,"S.No",range(1,len(df)+1))
                insert_table_black(df)
                if input("Would you like to continue for different  Timing [y] Yes").strip().lower()=="y":
                    continue
                else:
                    break
    except Exception as e:
        log_message(f"[ERROR] {e}")
        return None

def get_job_streams_df(db_key,tblst,job_stream_filter):
    creds=load_db_credentials()
    try:
        with  get_dynamic_db_connection(db_key, creds) as conn:
            log_message("Step 2: Connection established")
            with conn.cursor() as cur:
                log_message("Step 3: opened cursor")
                sql_qry=f""" SELECT * FROM BQ_JOB_STREAMS WHERE TARGET_TABLE_NAME IN {tblst}{job_stream_filter}
                AND ACTIVE_IND='Y'
                """
                log_message(f"sql query ->{sql_qry}")
                df=query_to_dataframe(cur,sql_qry)
        if df.empty:
            log_message("DataFrame is empty")
        else:
            return df
            log_message("DataFrame is not empty")
    except Exception as e:
        log_message(f"[ERROR] {e}")
        return None

def update_bq_job_streams(p_jct_col):
 try:

    # print("Select the column to be updated ")
    # print(f"1)update INCREMENTAL_COLUMN_NAME")
    # print(f"2)back date i.e update TO_EXTRACT_DTM")
    # print(f"3)update ACTVE_IND")
    jct_col=p_jct_col
    log_message(f"[DEBUG] value of jct_col:{jct_col}")

    print("Enetr the environment")
    print("     1)PRD\n              2)TS1\n              3)TS3")
    env=input().strip()
    if env=="1":
        db_key="EJCRO"
        env="PRD"
    elif  env=="2":
        db_key = "EJCTS1"
        env = "TS1"
    elif  env=="3":
        db_key = "EJCTS3"
        env = "TS3"

    no_of_days_backdate = None
    if jct_col=="3":
        incr_col=input("Enter incremental column name").upper().strip()
        print(f"the incremntal column name given is \n {incr_col}")
        jct_col_update=f"p_incremental_column_name=>'{incr_col}',"

    elif jct_col=="4":
        print("1)Backdate by no of days")
        print("2)Backdate by particular date")
        backdate_option=input("Enter the the choice")

        if   backdate_option=="1":
            while True:
                try:
                    no_of_days_backdate = int(input("Enter the number of days to be backdated: "))
                    break  # exit loop if conversion succeeds
                except ValueError:
                    print("❌ Invalid input. Please enter a valid number.")
        elif backdate_option=="2":
            while True:
                backdated_date = input("Enter the date to be backdated either in DD-MM-YYYY or YYYY-MM-DD: ")

                try:
                    # Try both formats
                    try:
                        parsed_date = datetime.strptime(backdated_date, "%d-%m-%Y")
                    except ValueError:
                        parsed_date = datetime.strptime(backdated_date, "%Y-%m-%d")

                    # Convert to desired format
                    formatted_date = parsed_date.strftime("%Y-%m-%d %H:%M:%S")
                    print(f"Formatted date: {formatted_date}")
                    break

                except ValueError:
                    print("❌ Invalid date format! Please enter in DD-MM-YYYY or YYYY-MM-DD format.")
            jct_col_update = f"p_to_extract_dtm=>TO_DATE('{parsed_date}', 'YYYY-MM-DD HH24:MI:SS'),"
            log_message(f"[DEBUG] JCT column update:=>{jct_col_update}")
    elif jct_col=="5":
        print("Would you like to be Disabled or Enabled")
        print("1)Enable")
        print("2)Disable")
        active_ind="Y" if input("Enter Choice").lower()=="1" else "N"
        jct_col_update=f"p_active_ind=>'{active_ind}',"


    ing_type=input("Enter ingestion type \n       1)FI\n       2)RI")
    job_stream_filter=" AND job_stream_id like '%TD'" if ing_type=="2" else " AND job_stream_id like '%BQ'"
    if jct_col=="3":
     tname=input("Enter table name ").upper().strip()
     tname = f"('{tname}')"
    else:
        input_table=input("Enter the table names in either comma separated or line separated").upper().strip()
        table_list=input_table.replace(",","\n").splitlines()
        tname="(" + ",".join([f"'{tname}'" for tname in table_list if tname.strip()]) + ")"
        log_message(f"[DEBUG] tname=>\n {tname}")

    creds=load_db_credentials()
    proc_bloc=[]
    log_message(f"[DEBUG] jct_col value =>{jct_col}")
    active_ind_filter=" AND active_ind='Y'" if jct_col!="5" else ""
    with  get_dynamic_db_connection(db_key, creds) as conn:
        with conn.cursor() as cur:
            qry=f""" select * from bq_job_streams where target_table_name in {tname}  {active_ind_filter}
                {job_stream_filter}
                ORDER BY target_table_name,decode(workflow_type,'SRC2STG',1,2)
                """
            log_message(f"[DEBUG] Framed query for job_streams\n {qry}")
            df = query_to_dataframe(cur,qry)
            df.insert(0,"S.No",range(1,len(df)+1))
            insert_table_black(df)
            ind=0
            if env == "1":
                    for index, row in df.iterrows():

                        js_id = row["JOB_STREAM_ID"]
                        wf_type_row = row["WORKFLOW_TYPE"]
                        if wf_type_row == 'SRC2STG' and jct_col in ["3", "4"]:
                                 if no_of_days_backdate is not None:
                                     parsed_date = row["TO_EXTRACT_DTM"] - timedelta(days=no_of_days_backdate)
                                     jct_col_update = f"p_to_extract_dtm=>TO_DATE('{parsed_date}', 'YYYY-MM-DD HH24:MI:SS'),"
                                 log_message("In if")
                                 ind = ind + 1
                                 block = f""" --{ind}
                                    EDS_DATA_CATALOG.xxedw_ingestion_utl_BQ.updateJCT(
                                        p_exec_env=>'{env}',
                                        p_job_stream_id=>'{js_id}',
                                        {jct_col_update}
                                        p_comments=>'JCT Adjustment for job group'
                                    );
                                            """
                                 proc_bloc.append(block)
                        elif  jct_col not in ["3", "4"]:
                            log_message("In elsif")
                            ind = ind + 1
                            block = f""" --{ind}
                               EDS_DATA_CATALOG.xxedw_ingestion_utl_BQ.updateJCT(
                                   p_exec_env=>'{env}',
                                   p_job_stream_id=>'{js_id}',
                                   {jct_col_update}
                                   p_comments=>'JCT Adjustment for job group'
                               );
                                               """
                            proc_bloc.append(block)


                # print(proc_bloc)
                    if proc_bloc:
                        final_block = "BEGIN \n" + "\n".join(proc_bloc) + "\nEND;"
                        # print(final_block)
                        copy_friendly_print(final_block)
                        # if input("Would you like to execute the above PLSQL block [y] Yes or [n] No").lower().strip()=='y':
                        #     try:
                        #         cur.execute(final_block)
                        #         conn.commit()
                        #         print("✅ PL/SQL block executed successfully.")
                        #     except Exception as e:
                        #         print("❌ Error executing block:", e)
                        # else:
                        #     return
                    if input("Would like to refresh the BQ_JOB_STREAM Table ").lower().strip()=="y":
                        df = query_to_dataframe(cur, qry)
                        insert_table_black(df)
                    else:
                        return
            else:
                  while True:
                     if jct_col=="3":
                         qry=f"""UPDATE BQ_JOB_STREAMS SET INCREMENTAL_COLUMN_NAME='{incr_col}'
                                    WHERE TARGET_TABLE_NAME IN {tname} AND ACTIVE_IND='Y'
                                    {job_stream_filter} 
                                    and workflow_type='SRC2STG'
                              """
                     elif jct_col=="4":
                         if backdate_option=="1":
                          qry=f""" update bq_job_streams set to_extract_dtm=to_extract_dtm -{no_of_days_backdate}
                                    where TARGET_TABLE_NAME IN {tname} 
                                    and active_ind='Y' {job_stream_filter} 
                                    and workflow_type='SRC2STG'
                             """
                         elif   backdate_option=="2":
                             qry = f""" update bq_job_streams set to_extract_dtm=TO_DATE('{parsed_date}', 'YYYY-MM-DD HH24:MI:SS')
                                       where TARGET_TABLE_NAME IN {tname}
                                       and active_ind='Y' {job_stream_filter} 
                                       and workflow_type='SRC2STG'
                                """

                     copy_friendly_print(qry)
                     if input("would you like to repaet for any other table[y] Yes").lower().strip()=="y":
                        if jct_col=="3":
                            incr_col = input("Enter incremental column name").upper().strip()
                            print(f"the incremntal column name given is \n {incr_col}")
                            tname = input("Enter table name ").upper().strip()
                            tname = f"('{tname}')"
                        elif jct_col=="4":
                            input_table = input(
                                "Enter the table names in either comma separated or line separated").upper().strip()
                            table_list = input_table.replace(",", "\n").splitlines()
                            tname = "(" + ",".join([f"'{tname}'" for tname in table_list if tname.strip()]) + ")"
                            print(f"tname=>\n {tname}")

                            if backdate_option == "1":
                                while True:
                                    try:
                                        no_of_days_backdate = int(input("Enter the number of days to be backdated: "))
                                        break  # exit loop if conversion succeeds
                                    except ValueError:
                                        print("❌ Invalid input. Please enter a valid number.")

                            elif backdate_option == "2":
                                while True:
                                    backdated_date = input(
                                        "Enter the date to be backdated either in DD-MM-YYYY or YYYY-MM-DD: ")

                                    try:
                                        # Try both formats
                                        try:
                                            parsed_date = datetime.strptime(backdated_date, "%d-%m-%Y")
                                        except ValueError:
                                            parsed_date = datetime.strptime(backdated_date, "%Y-%m-%d")

                                        # Convert to desired format
                                        formatted_date = parsed_date.strftime("%Y-%m-%d %H:%M:%S")
                                        print(f"Formatted date: {formatted_date}")
                                        break

                                    except ValueError:
                                        print("❌ Invalid date format! Please enter in DD-MM-YYYY or YYYY-MM-DD format.")
                                continue
                     else:
                         break
 except Exception as e:
     import traceback
     log_message(f"[ERROR] ): {e}")
     traceback.print_exc()
     return None

def build_first_data_query(src_schema, src_table, missing_str,incr_col):
    cols = [c.strip() for c in missing_str.split(",")]
    queries = []
    for c in cols:
        q = f"""
        SELECT '{c}' AS column_name,
               MIN({incr_col}) AS first_data_date
        FROM {src_schema}.{src_table}
        WHERE {c} IS NOT NULL
        """
        queries.append(q)
    return " UNION ALL ".join(queries)

def query_to_dataframe(cursor, query: str, normalize_numeric: bool = True):
    """
    Executes a SQL query using a DBAPI cursor and returns a cleaned pandas DataFrame.

    ✅ Removes scientific notation (e.g., 0E-7 → 0)
    ✅ Converts Decimals to int or float properly
    ✅ Keeps NULLs as NaN
    ✅ Automatically sets proper dtypes
    """
    # log_message(f"[DEBUG] query sent for the query_to_dataframe function \n {query}")
    log_message(f"[INFO] inside query_to_dataframe() function")
    cursor.execute(query)
    rows = cursor.fetchall()
    col_names = [desc[0] for desc in cursor.description]

    if normalize_numeric:
        def normalize_value(val):
            if val is None:
                return np.nan
            if isinstance(val, decimal.Decimal):
                # Convert Decimal to int or float based on its value
                if val == val.to_integral_value():
                    return int(val)
                else:
                    return float(val)
            return val

        rows = [[normalize_value(v) for v in row] for row in rows]

    df = pd.DataFrame(rows, columns=col_names)

    # Optional: improve display readability
    pd.set_option('display.float_format', lambda x: f'{x:.6f}')
    log_message(f"[INFO] retruning the DF from query_to_dataframe() function")
    return df

def compare_metadata(env_name,tname):
    creds=load_db_credentials()
    if env_name=='PRD':
        db_key="EJCRO"
    elif env_name=="TS1":
        db_key = "EJCTS1"
    elif env_name == "TS1":
        db_key = "EJCTS3"
    while True:
        with   get_dynamic_db_connection(db_key, creds) as conn:
            with  conn.cursor() as cur:
                qry = f""" select * from bq_job_streams where target_table_name in ('{tname}') AND job_stream_id like '%BQ'
                        --AND active_ind='Y'
                        """
                log_message(f"[DEBUG] query used for getting job stream data \n        {qry}")
                df = pd.read_sql(qry, conn)
                if df.empty:
                    print("no rows returned,Hence going back")
                    return;
                df = df[df["WORKFLOW_TYPE"] == 'SRC2STG'].iloc[0]
                src_db_name = df["SOURCE_DB_NAME"]
                src_schema = df["SOURCE_SCHEMA"]
                src_table_name = df["TARGET_TABLE_NAME"]
                src_conn_name=df["SOURCE_DB_CONNECTION"]
                incr_col=df["INCREMENTAL_COLUMN_NAME"]

                sql1 = f"""
                select distinct decode(parameter_category,'HANANRT','HANA',parameter_category) 
                from EDS_DATA_CATALOG.EDW_PARAMETER  where upper(environment_name)='{env_name}' 
                and parameter_type ='{src_conn_name}' """
                cur.execute(sql1)
                results = cur.fetchall()
                for obj in results:
                    dbType = obj[0]
        if dbType == 'ORACLE':
            with get_connection(env_name,src_table_name) as src_db_conn:
                sql1 = f""" SELECT COLUMN_NAME,COLUMN_ID,DATA_TYPE,DATA_LENGTH,DATA_PRECISION,DATA_SCALE,NULLABLE ,data_default , null as CONSTRAINT_TYPE,
                        null as CONSTRAINT_VALUE FROM ALL_TAB_COLUMNS where owner= '{src_schema}' and table_name='{src_table_name}' order by column_id"""
                src_col_df=pd.read_sql(sql1,src_db_conn)
                src_col_df.insert(0, "SNO", range(1, len(src_col_df) + 1))
                # print("Source Column Data")
                # insert_table_black(src_col_df)
                # print("Now displaying metadata")
                metadata_df=display_metadata(creds,db_key,env_name,tname,'FI')
                # insert_table_black(metadata_df)
                col1 = src_col_df[['COLUMN_NAME']].rename(columns={'COLUMN_NAME': 'SRC_COLUMNS'})
                col2 = metadata_df[['COLUMN_NAME']].rename(columns={'COLUMN_NAME': 'METADATA_COLUMNS'})

                # Step 2: Full outer join on the column values
                merged = pd.merge(col1, col2, left_on='SRC_COLUMNS', right_on='METADATA_COLUMNS', how='outer')

                # Step 3: Add status column
                merged['Status'] = merged.apply(
                    lambda x: '✅' if x['SRC_COLUMNS'] == x['METADATA_COLUMNS'] else '❌',
                    axis=1
                )
                print("Please find the comparison of src and metadata")
                insert_table_black(merged)


        elif dbType == 'TERADATA':
            with get_connection(env_name, src_table_name) as src_db_conn:
                sql1 = f"""select c.column_name,column_sequence_int,data_type_code,
                                    data_type_character_width_int,data_type_deciml_precision_int,data_type_decimal_scale_int, nvl(NULL_ALLOWED_FLAG,'Y') null_allowed_flag,trim(OReplace(default_value_text, '''', '')) AS default_value , CASE WHEN I.IndexType in ('P','Q') and UniqueFlag ='Y'  THEN  'UNIQUE PRIMARY INDEX'
                                    WHEN I.IndexType in ('P','Q') and UniqueFlag ='N' THEN 'NON UNIQUE PRIMARY INDEX'
                                    ELSE NULL END constraint_type, case when constraint_type is not null then c.column_name   else null end as constraint_value
                                from etloffloaddb.dbc_columns c
                                LEFT JOIN dbc.indicesv I
                                ON (i.databasename = c.database_name and i.tablename = c.table_name and i.columnname = c.column_name and I.indextype in ('Q','P' ) )
                                    where c.database_name='{src_schema}' and c.table_name='{src_table_name}' order by COLUMN_SEQUENCE_INT"""
                src_col_df = pd.read_sql(sql1, src_db_conn)
                src_col_df.insert(0, "SNO", range(1, len(src_col_df) + 1))
                # print("Source Column Data")
                # insert_table_black(src_col_df)
                # print("Now displaying metadata")
                metadata_df = display_metadata(creds, db_key, env_name, tname, 'FI')
                # insert_table_black(metadata_df)
                col1 = src_col_df[['COLUMN_NAME']].rename(columns={'COLUMN_NAME': 'SRC_COLUMNS'})
                col2 = metadata_df[['COLUMN_NAME']].rename(columns={'COLUMN_NAME': 'METADATA_COLUMNS'})

                # Step 2: Full outer join on the column values
                merged = pd.merge(col1, col2, left_on='SRC_COLUMNS', right_on='METADATA_COLUMNS', how='outer')

                # Step 3: Add status column
                merged['Status'] = merged.apply(
                    lambda x: '✅' if x['SRC_COLUMNS'] == x['METADATA_COLUMNS'] else '❌',
                    axis=1
                )
                merged = merged.sort_values(by="Status", ascending=True)
                print("Please find the comparison of src and metadata")
                insert_table_black(merged)
                if input("would you like to repeat for any other table [y] yes or [n] no").lower()=="y":
                    tname=input("enter table name").strip().upper()
                    continue

        missing_in_metadata = merged.loc[merged["Status"] == "❌", "SRC_COLUMNS"]

        # Convert to comma-separated string
        missing_str = ", ".join(missing_in_metadata[missing_in_metadata != ""].tolist())
        log_message(f"[INFO] missing columns in metadata {missing_str}")
        print("Now lets see how is the data coming in the new columns")

        qry=f"""SELECT CAST(COUNT(*) AS DECIMAL(18,0)) AS cnt
                FROM (
                    SELECT DISTINCT {missing_str}
                    FROM {src_schema}.{src_table_name}
                ) AS x;
            """
        # qry=f"""select  cast(count(distinct {missing_str}) as bigint) from {src_schema}.{src_table_name}
        #    """
        log_message(f"[DEBUG] Framed Query \n {qry}")
        count_val=0
        with (get_connection(env_name, src_table_name) as conn):
            cursor = conn.cursor()

            # 1️⃣ Count query
            qry_count = f"""
                SELECT CAST(COUNT(*) AS DECIMAL(18,0)) AS cnt
                FROM (
                    SELECT DISTINCT {missing_str}
                    FROM {src_schema}.{src_table_name}
                ) AS x
            """

            try:
                df_count = query_to_dataframe(cursor, qry_count)
                count_val = int(df_count.iloc[0, 0])
                log_message(f"[INFO] Number of distinct rows for the above query: {count_val}")
            except Exception as e:
                log_message(f"[ERROR] error=>{e}")

            # 2️⃣ Distinct values if few
            if count_val == 1:
                qry_distinct = f"""
                    SELECT DISTINCT {missing_str}
                    FROM {src_schema}.{src_table_name}
                """
                try:
                    distinct_values_df = query_to_dataframe(cursor, qry_distinct)
                except Exception as e:
                    log_message(f"[ERROR] error=>{e}")
                print("We have few values for the newly added columns as below:")

                insert_table_black(distinct_values_df)
                # print("Now, let's understand from what date these started coming...")
                # qry = build_first_data_query(src_schema, src_table_name, missing_str,incr_col)
                # df = pd.read_sql(qry, conn)
                # insert_table_black(df)
                break
                if len(df)=="1":
                    None

            elif count_val > 1:
                log_message(f"[INFO] There are {count_val} distinct combinations for the newly added columns.")
                qry = build_first_data_query(src_schema, src_table_name, missing_str,incr_col)
                if incr_col is None:
                    log_message(f"[ERROR] No incrementa column exisitng either do full load or add incremental column")
                    return
                log_message(f"[DEBUG] Query framed to check the minimum date for the all the newly added columns \n  {qry}")
                df = pd.read_sql(qry, conn)
                insert_table_black(df)
                min_date = df["first_data_date"].min()
                if input("Do you want to print the min date of all the column [y] yes or [n] no").lower()=="y":
                    print(f"min date:{min_date}")
                    # insert_table_black(min_date)
                print("lets understand the total records and the data first updated date")
                qry = f"""
                    SELECT 
                        CAST(COUNT(*) AS DECIMAL(18,0)) AS cnt,
                        MIN({incr_col}) AS min_value
                    FROM {src_schema}.{src_table_name}
                """

                all_data_min_cnt_df = query_to_dataframe(cursor, qry)
                insert_table_black(all_data_min_cnt_df)

                # assuming query_to_dataframe returns a pandas DataFrame
                total_rec = all_data_min_cnt_df.iloc[0, 0]
                min_of_all_data = all_data_min_cnt_df.iloc[0, 1]

                print("Total records:", total_rec)
                print("Minimum date/value:", min_of_all_data)
                print("lets understand how many records are not updated before the first appeared data for the new columns")
                if isinstance(min_date, pd.Timestamp):
                    date_condition = f"TIMESTAMP '{min_date.strftime('%Y-%m-%d %H:%M:%S')}'"
                else:
                    # fallback if it's string or already timestamp-like
                    date_condition = f"TIMESTAMP '{str(min_date)}'"

                qry = f"""
                    SELECT 
                        CAST(COUNT(*) AS DECIMAL(18,0)) AS cnt
                    FROM {src_schema}.{src_table_name}
                    WHERE {incr_col} < {date_condition}
                """
                log_message("Final query prepared")
                blank_data_df = query_to_dataframe(cursor, qry)
                blank_rec = blank_data_df.iloc[0, 0]
                log_message(f"[INFO] There are {blank_rec} records which are blank")
                # insert_table_black(blank_data_df)
                # print(f"lets understand how many records are blank out of {total_rec}")
                blank_cols_cnt=blank_rec
                # print(f"We see that the blank records are:{blank_cols_cnt}")
                percentage_of_blank=(blank_cols_cnt/total_rec)*100;
                log_message(f"[INFO] Percentage of blank data: {percentage_of_blank}")
                percentange_filled=100 - percentage_of_blank
                log_message(f"[INFO] Percentage of value in the updated for the newly added columns {percentange_filled}")
                if percentange_filled > 80:
                    if env_name!='PRD':
                        print("more than 80% data is updated for the newly added columns so it is better to go for the full load")
                        print("use the below query to update the JCT")
                        qry=f""" UPDATE BQ_JOB_STREAMS SET RUN_STATUS='P',
                                EXTRACT_TYPE=DECODE(WORKFLOW_TYPE,'SRC2STG','ALL_DATA',EXTRACT_TYPE)
                                WHERE TARGET_TABLE_NAME='{src_table_name}' AND JOB_STREAM_ID LIKE '%BQ'
                                AND ACTIVE_IND='Y'
                                """
                        copy_friendly_print(qry)
                        log_message("[DEBUG] now,lets use the below JCT to run the ingestion")
                        with   get_dynamic_db_connection(db_key, creds) as conn:
                            qry = f"""SELECT get_jct('{src_table_name}') AS jct FROM dual"""
                            df = pd.read_sql(qry, conn)
                            value= df.iloc[0, 0]
                            copy_friendly_print(value)
                            break
                    else:
                        print("more than 80% data is updated for the newly added columns so it is better to go for the full load")
                        full_load_Setup(creds,'n')
                        break


            else:
                log_message("[INFO] Distinct count is exactly 1 — no variance detected.")
                break

def get_connection_name(env_name,tname):
    if env_name=='PRD':
        db_key='EJCRO'
    elif env_name=="TS1":
        db_key='EJCTS1'
    elif env_name=="TS3":
        db_key="EJCTS3"
    creds=load_db_credentials()
    with get_dynamic_db_connection(db_key, creds) as conn:
        from parity_analysis import get_job_streams_for_tables
        df = get_job_streams_for_tables(conn, [tname], 'FI')
        src_row = df[df["WORKFLOW_TYPE"] == "SRC2STG"]
        conn_name = src_row["SOURCE_DB_CONNECTION"].iloc[0]
        return conn_name
    # df=get_job_streams_for_tables(conn, [tname], 'FI')

def get_connection(env_name,tname=None,conn=None):
    if conn is None:
     conn_name=get_connection_name(env_name,tname)
    else:
        conn_name=conn
    creds = load_db_credentials()
    with get_dynamic_db_connection('EJCRO', creds) as ejcro_conn:
        query = f"""select parameter_name,to_char(parameter_value) parameter_value,
                CASE WHEN PARAMETER_NAME = 'SOURCE_LOGIN_PASSWORD'
                THEN UTL_I18N.RAW_TO_CHAR(to_char(a.parameter_value),'AL32UTF8')
                ELSE '' END PASSWORD from EDS_DATA_CATALOG.EDW_PARAMETER a
                where upper(environment_name)=:1 and parameter_type =:2
                """
        cur=ejcro_conn.cursor()
        result = cur.execute(query, (env_name, conn_name))
        rows=cur.fetchall()
        column = [desc[0] for desc in cur.description]
        cred_df=pd.DataFrame(rows,columns=column)
        log_message(f"[INFO] conn name passed: {conn_name}")

        if len(cred_df)==5:
            config = {
                "type": "oracle",
                "host": cred_df[cred_df["PARAMETER_NAME"] == "SOURCE_HOST"].iloc[0]["PARAMETER_VALUE"],
                "user": cred_df[cred_df["PARAMETER_NAME"] == "SOURCE_LOGIN"].iloc[0]["PARAMETER_VALUE"],
                "password": cred_df[cred_df["PARAMETER_NAME"] == "SOURCE_LOGIN_PASSWORD"].iloc[0]["PASSWORD"],
                "service": cred_df[cred_df["PARAMETER_NAME"] == "SOURCE_SERVICE_NAME"].iloc[0]["PARAMETER_VALUE"],
                "port": cred_df[cred_df["PARAMETER_NAME"] == "SOURCE_PORT"].iloc[0]["PARAMETER_VALUE"]
            }
        elif  len(cred_df)==4:

            config= {
                "type": "teradata",
                "host": cred_df[cred_df["PARAMETER_NAME"] == "SOURCE_HOST"].iloc[0]["PARAMETER_VALUE"],
                "user": cred_df[cred_df["PARAMETER_NAME"] == "SOURCE_LOGIN"].iloc[0]["PARAMETER_VALUE"],
                "password": cred_df[cred_df["PARAMETER_NAME"] == "SOURCE_LOGIN_PASSWORD"].iloc[0]["PASSWORD"]
                }
        # log_message(f"[INFO] Built config dynamically for {db_key}: {config}")

        # now config must be a dict
    db_type = config.get("type").lower()
    log_message(f"[INFO] Detected DB type for '{conn_name}': {db_type}")

    try:
        if db_type == "oracle":
            dsn = cx_Oracle.makedsn(
                config["host"],
                int(config["port"]),  # ensure port is int
                service_name=config["service"]
            )
            log_message(f"[INFO] Connecting to Oracle {conn_name} as {config['user']}...")
            conn = cx_Oracle.connect(
                user=config["user"],
                password=config["password"],
                dsn=dsn
            )
            log_message(f"[INFO] Oracle connection to '{conn_name}' successful.")
        elif db_type == "teradata":
            log_message(f"[INFO] Connecting to Teradata {conn_name} as {config['user']}...")
            conn = teradatasql.connect(
                host=config["host"],
                user=config["user"],
                password=config["password"]
            )
            log_message(f"[INFO] Teradata connection to '{conn_name}' successful.")
        else:
            log_message(f"[ERROR] Unsupported DB type: {db_type}")
            return None

        return conn
    except Exception as e:
        import traceback
        log_message(f"[ERROR] Failed to connect to '{conn_name}' ({db_type}): {e}")
        traceback.print_exc()
        return None


def set_status_to_p(creds):
    full_load_Setup(creds,'y')

def generate_merge_key(creds):
    print(f"{INDENT}Environment \n    1)PROD \n    2)NON-PROD")
    env_choice = input("Enter the Choice").strip()
    if env_choice == "1":
        tname = input("Enter Table name").strip().upper()
        db_key = 'EJCRO'
        env_name = 'PRD'
    elif env_choice == "2":
        print("Enter which sub environment \n    1)TS1 \n    2)TS3")
        env_sub_choice = input("Enter the Choice").strip()
        tname = input("Enter Table name").strip().upper()
        if env_sub_choice == "1":
            db_key = 'EJCTS1'
            env_name = 'TS1'
        elif env_sub_choice == "2":
            db_key = 'EJCTS3'
            env_name = 'TS3'
    job_filter = input("Enter the ingestion type \n    1)FI \n    2)RI").strip()
    job_stream_filter = " job_stream_id like '%BQ'" if job_filter == "1" else " job_stream_id like '%TD'"
    col_names=input("provide merge key column names either comma separated or line separated").strip()
    col_names_list = [tbl.strip().upper() for tbl in col_names.replace(",", "\n").splitlines() if tbl.strip()]
    final_col_names_list=",".join(col_names_list)
    if not col_names_list:
        print(f"{INDENT}❌ No column names provided.")
        return
    with   get_dynamic_db_connection(db_key, creds) as conn:
        with  conn.cursor() as cur:
            qry = f""" select * from bq_job_streams where target_table_name in ('{tname}') AND {job_stream_filter}
                    """
            df = pd.read_sql(qry, conn)
            if df.empty:
                print("no rows returned,Hence going back")
                return;
            df = df[df["WORKFLOW_TYPE"] == 'SRC2STG'].iloc[0]
            src_db_name = df["SOURCE_DB_NAME"]
            src_schema = df["SOURCE_SCHEMA"]
            src_table_name = df["SOURCE_TABLE_NAME"]
            blk=f""" 
    BEGIN
        EDS_DATA_CATALOG.xxedw_ingestion_utl_BQ.createUKEXP(p_exec_env=>'{env_name}',
        p_db_instance_name =>'{src_db_name}',
        p_db_schema_name   =>'{src_schema}',
        p_table_name       =>'{src_table_name}',
        p_column           =>'{final_col_names_list}',
        p_comments         =>'Added pks as additional key column');
    END;"""
    print("merge key addition script")
    copy_friendly_print(blk)

def display_metadata(creds,db_key=None,env_name=None,tabname=None,job_stream_filter=None):
    if db_key is None:
        print(f"{INDENT}Environment \n    1)PRD \n    2)NON-PROD")
        env_choice=input("Enter the Choice").strip()
        if env_choice=="1":
            tname=input("Enter Table name").strip().upper()
            db_key='EJCRO'
            env_name='PRD'
        elif   env_choice=="2":
            print("Enter which sub environment \n    1)TS1 \n    2)TS3")
            env_sub_choice=input("Enter the Choice").strip()
            tname=input("Enter Table name").strip().upper()
            if env_sub_choice=="1":
                db_key = 'EJCTS1'
                env_name = 'TS1'
            elif   env_sub_choice=="2":
                db_key = 'EJCTS3'
                env_name = 'TS3'
        job_filter=input("Enter the ingestion type \n    1)FI \n    2)RI").strip()
        job_stream_filter = " job_stream_id like '%BQ'" if job_filter == "1" else " job_stream_id like '%TD'"
    # if job_stream_filter is not None:
    #     job_stream_filter = " job_stream_id like '%BQ'"
    with   get_dynamic_db_connection(db_key, creds) as conn:
        if conn is None:
            print(f"⚠️ Could not establish connection for {db_key}. Skipping...")
            return
        with  conn.cursor() as cur:
            # print("Inside with conn.cursor")
         while True:
            if tabname is  None:
                tname = input("Enter Table name").strip().upper()
            if  tabname is  not None:
                tname=tabname
                job_stream_filter = " job_stream_id like '%BQ'"
            qry=f""" select * from bq_job_streams where target_table_name in ('{tname}') AND {job_stream_filter}
                """
            df=pd.read_sql(qry,conn)
            # log_message(f"[DEBUG] after executing query:{qry}")
            if df.empty:
                print("no rows returned,Hence going back")
                return
            df=df[df["WORKFLOW_TYPE"]=='SRC2STG'].iloc[0]
            src_db_name=df["SOURCE_DB_NAME"]
            src_schema=df["SOURCE_SCHEMA"]
            src_table_name = df["SOURCE_TABLE_NAME"]

            qry1= f""" SELECT *
    FROM EDS_DATA_CATALOG.EDWBQ_TABLE_COLUMN etc
    WHERE 1=1
    AND DB_INSTANCE_NAME ='{src_db_name}'
    AND DB_SCHEMA_NAME = '{src_schema}'
    AND TABLE_NAME LIKE '{src_table_name}'
    AND ENVIRONMENT_NAME = '{env_name}' order by column_sequence
          """
            print(f" executing query:{qry1}")
            df1 = pd.read_sql(qry1, conn)
            if df1.empty:
                print("no rows returned,Hence going back")
                return;
            else:
                df1.insert(0,"SNO",range(1,len(df1)+1))
                insert_table_black(df1)
            if  tabname is  None:
                if input("Would you like to check any other tables [y] Yes or [n] No ").strip().lower() == "y":
                    continue;
                else:
                    break
            else:
                break
        return df1

def full_load_Setup(creds,run_status):

        env=input("Enter the Environment \n       1)PRD \n       2)TS1 \n       3)TS3").strip().lower()
        if env=="1":
            db_key='EJCRO'
        elif env=="2":
            db_key='EJCTS1'
        elif env=="3":
            db_key='EJCTS3'
        else:
            print("INVALID CHOICE, Returning back" )
            return


        tnames = input("Enter the table Names either in comma separated or new line separated: ").strip().upper()
        tnames=tnames.replace(',','\n')
        input_table_list=tnames.splitlines()
        table_list=[]
        for tname in input_table_list:
            if tname.strip():
                table_list.append(tname)

        ing_type = input(" Enter the ingestion type \n      1)FI \n      2)RI: ").strip()

        if ing_type not in ["1", "2"]:
            print("❌ Invalid Choice")
            return

        job_stream_filter = " job_stream_id like '%BQ'" if ing_type == "1" else " job_stream_id like '%TD'"
        # proc_bloc = []
        # final_block=""
        # sql_qry=""
        conn =None
        with get_dynamic_db_connection(db_key, creds) as conn:
            with conn.cursor() as cur:
                while True:
                    dfs = pd.DataFrame()
                    proc_bloc = []
                    final_block = ""
                    sql_qry = ""
                    if env=="1":
                        ind = 0
                        for table_name in table_list:
                            qry = f"""
                               SELECT * 
                               FROM bq_job_streams 
                               WHERE target_table_name = '{table_name}'
                                 AND {job_stream_filter} --AND active_ind='Y'
                           """
                            # log_message(f"[DEBUG] Framed query: {qry}")
                            df = pd.read_sql(qry, conn)
                            dfs = pd.concat([dfs, df], ignore_index=True)

                            if df.empty:
                                print(f"❌ No matching entry found in bq_job_streams for table '{tname}'")
                                final_block = ""
                            else:
                                # log_message(f"[INFO] length of DF{len(df)}")

                                for index, row in df.iterrows():
                                    js_id = row["JOB_STREAM_ID"]
                                    wf_type = row["WORKFLOW_TYPE"]

                                    # ✅ inline condition for workflow-specific params
                                    wf_params = (
                                        "\n           ,p_extract_type=>'ALL_DATA'"
                                        if wf_type.upper() == "SRC2STG"
                                        else "\n           ,p_merge_type=>'OVERWRITE'"
                                    )
                                    if run_status=="y":
                                        wf_params=""


                                    block = f""" --{ind + 1}
         EDS_DATA_CATALOG.xxedw_ingestion_utl_BQ.updateJCT(
            p_exec_env=>'PRD'
           ,p_job_stream_id=>'{js_id}'{wf_params}
           ,p_run_status=>'P'
           ,p_comments=>'JCT Adjustment for job group');
            """
                                    proc_bloc.append(block)
                                    ind=ind+1
                                    # log_message(f'[DEBUG] ind value:{ind}')

                                final_block = "BEGIN \n" + "\n".join(proc_bloc) + "\nEND;"
                        insert_table_black(dfs)
                        if final_block:
                            copy_friendly_print(final_block)
                    elif env=="2":

                        log_message(f"[DEBUG] input table list->{input_table_list}")
                        table_names="(" + ",".join([f"'{tname.strip()}'" for tname in input_table_list if tname.strip() ]) +")"
                        log_message(f"[DEBUG] table_names->{table_names}")

                        qry = f"""
                            SELECT * 
                            FROM bq_job_streams 
                            WHERE target_table_name IN {table_names}
                              AND {job_stream_filter} --AND active_ind='Y'
                        """
                        # log_message(f"[DEBUG] Framed query: {qry}")
                        df = pd.read_sql(qry, conn)
                        if df.empty:
                            print(f"❌ No matching entry found in bq_job_streams for table '{tname}'")
                        else:
                            insert_table_black(df)

                        sql_qry=f"""
UPDATE BQ_JOB_STREAMS SET RUN_STATUS='P',EXTRACT_TYPE=DECODE(WORKFLOW_TYPE,'SRC2STG','ALL_DATA',EXTRACT_TYPE) 
WHERE TARGET_TABLE_NAME IN {table_names} and {job_stream_filter} AND ACTIVE_IND='Y'
 """
                        copy_friendly_print(sql_qry)
                    if final_block or sql_qry:
                        code=final_block if final_block else sql_qry
                        if input(f"would you like to Execute Above Code in {db_key}?").lower().strip()=="y":
                            user_choice = ask_confirmation(
                                f"Are you sure you want to execute above SQL query/Block in {db_key}?",
                                title="Confirm Execution"
                            )

                            if user_choice == 'Y':
                                try:
                                    cur.execute(code)
                                    conn.commit()
                                    print("✅ Query/Block executed successfully.")
                                except Exception as e:
                                    print("❌ Error executing Query:", e)
                        else:
                            print(f"Since user do not want the block to be Executed, Going to next steps")


                        # log_message(f"[DEBUG] run status: {run_status}")
                    if run_status != "y":
                        if input(
                                "Would you like to generate full load setup script for any other tables (y/n)? ").strip().lower() != "y":
                            break
                        else:
                            tnames = input(
                                "Enter the table Names either in comma separated or new line separated: ").strip().upper()
                            tnames = tnames.replace(',', '\n')
                            input_table_list = tnames.splitlines()
                            table_list = []
                            for tname in input_table_list:
                                if tname.strip():
                                    table_list.append(tname)
                            continue
                    else:
                        ans= input("Would you like to set C to P for any other table [y] yes [n] no").lower().strip()
                        if ans=="y":
                            tnames = input(
                                "Enter the table Names either in comma separated or new line separated: ").strip().upper()
                            tnames = tnames.replace(',', '\n')
                            input_table_list = tnames.splitlines()
                            table_list = []
                            for tname in input_table_list:
                                if tname.strip():
                                    table_list.append(tname)
                            continue
                        else:
                            break
        ans=input("Would you like to get the JCT for the above tables [y] Yes or [n] No ").lower().strip()
        if ans=='y':
            tnames = tnames.strip("()").replace("'", "").replace("\n",",")
            log_message(f"[DEBUG] List of Tables \n {tnames} ")
            with get_dynamic_db_connection(db_key, creds) as conn:
                with conn.cursor() as cur:
                    if ing_type == "1":
                        log_message(f"[DEBUG] calling get_jct for FI")
                        value = cur.callfunc("get_jct", str, [tnames])
                    else:
                        log_message(f"[DEBUG] calling get_jct for RI")
                        value = cur.callfunc("get_jct_ri", str, [tnames])
                    copy_friendly_print(value)

def load_db_credentials():
    with open("db_cred.json") as f:
        return json.load(f)

def fetch_bq_count_and_max_ts(fq_table, incr_col, key_path):
    try:
        client = bigquery.Client.from_service_account_json(key_path)
        query = f"SELECT COUNT(*) as total, MAX({incr_col}) as latest_ts  FROM `{fq_table}`"
        print(f"{INDENT}⏳ Running: {query}")
        result = client.query(query).result()
        for row in result:
            print(f"{INDENT}✔ Total Rows: {row['total']}")
            print(f"{INDENT}✔ Latest Timestamp ({incr_col}): {row['latest_ts']}")
    except Exception as e:
        log_message(f"{INDENT}❌ Failed to query BigQuery: {e}")

def fetch_count_and_max_ts(fq_table, incr_col):
    creds = load_db_credentials()
    td_config = creds.get("TDPROD")
    if not td_config:
        log_message(f"{INDENT}❌ TDPROD credentials not found in db_cred.json")
        return

    try:
        conn = teradatasql.connect(
            host=td_config["host"],
            user=td_config["user"],
            password=td_config["password"]
        )
        cursor = conn.cursor()
        query = f"SELECT COUNT(*), MAX({incr_col}) FROM {fq_table}"
        log_message(f"{INDENT}⏳ Running: {query}")
        cursor.execute(query)
        row = cursor.fetchone()
        log_message(f"{INDENT}✔ Total Rows: {row[0]}")
        log_message(f"{INDENT}✔ Latest Timestamp ({incr_col}): {row[1]}")
        cursor.close()
        conn.close()
    except Exception as e:
        log_message(f"{INDENT}❌ Failed to query TDPROD: {e}")

def get_db_connection(env, creds):
    if env == "1":
        config = creds.get("EJCRO")
    elif env == "2":
        print(f"{INDENT}Choose NONPROD type:")
        print(f"{INDENT}1) TS1")
        print(f"{INDENT}2) TS3")
        ts_choice = input(f"{INDENT}Enter your choice: ").strip()
        if ts_choice == "1":
            config = creds.get("EJCTS1")
        elif ts_choice == "2":
            config = creds.get("EJCTS3")
        else:
            print(f"{INDENT}Invalid NONPROD option.")
            return None
    else:
        print(f"{INDENT}Invalid environment choice.")
        return None

    if not config:
        print(f"{INDENT}Missing DB config.")
        return None

    try:
        dsn = cx_Oracle.makedsn(config["host"], config["port"], service_name=config["service"])
        conn = cx_Oracle.connect(user=config["user"], password=config["password"], dsn=dsn)
        return conn
    except Exception as e:
        log_message(f"{INDENT}❌ Failed to connect to DB: {e}")
        return None

# ✅ NEW FUNCTION: dynamic DB connector
def get_dynamic_db_connection(db_key, creds):
    from general_menu import find_db_creds

    log_message(f"[INFO] Attempting to fetch DB config for '{db_key}' ...")
    config = creds.get(db_key)

    # If not found in creds, lookup via EJCRO
    if not config:
        conn1 = get_dynamic_db_connection("EJCRO", creds)
        if not conn1:
            log_message("[ERROR] Could not establish connection to EJCRO")
            return None

        with conn1.cursor() as cur:
            qry = """
                SELECT DISTINCT SOURCE_DB_CONNECTION
                FROM bq_job_streams
                WHERE source_db_name = :db_name
            """
            cur.execute(qry, db_name=db_key)
            res = cur.fetchall()
            if not res:
                log_message(f"[ERROR] No SOURCE_DB_CONNECTION found for {db_key}")
                return None
            conn_name1 = res[0][0]

        # fetch parameters for conn_name1
        cred_df = find_db_creds(creds, "PRD", conn_name1)
        config = {
            "type": "oracle",
            "host": cred_df[cred_df["PARAMETER_NAME"] == "SOURCE_HOST"].iloc[0]["PARAMETER_VALUE"],
            "user": cred_df[cred_df["PARAMETER_NAME"] == "SOURCE_LOGIN"].iloc[0]["PARAMETER_VALUE"],
            "password": cred_df[cred_df["PARAMETER_NAME"] == "SOURCE_LOGIN_PASSWORD"].iloc[0]["PASSWORD"],
            "service": cred_df[cred_df["PARAMETER_NAME"] == "SOURCE_SERVICE_NAME"].iloc[0]["PARAMETER_VALUE"],
            "port": cred_df[cred_df["PARAMETER_NAME"] == "SOURCE_PORT"].iloc[0]["PARAMETER_VALUE"]
        }

    # now config must be a dict
    db_type = config.get("type", "oracle").lower()
    log_message(f"[INFO] Detected DB type for '{db_key}': {db_type}")

    try:
        if db_type == "oracle":
            if config["service"]=='CRTPRD.cisco.com':
             config["service"]='CRTPRD'
            dsn = cx_Oracle.makedsn(
                config["host"],
                int(config["port"]),  # ensure port is int
                service_name=config["service"]
            )
            log_message(f"[INFO] Connecting to Oracle {db_key} as {config['user']}...")
            conn = cx_Oracle.connect(
                user=config["user"],
                password=config["password"],
                dsn=dsn
            )
            log_message(f"[INFO] Oracle connection to '{db_key}' successful.")
        elif db_type == "teradata":
            log_message(f"[INFO] Connecting to Teradata {db_key} as {config['user']}...")
            conn = teradatasql.connect(
                host=config["host"],
                user=config["user"],
                password=config["password"]
            )
            log_message(f"[INFO] Teradata connection to '{db_key}' successful.")
        else:
            log_message(f"[ERROR] Unsupported DB type: {db_type}")
            return None

        return conn

    except Exception as e:
        import traceback
        log_message(f"[ERROR] Failed to connect to '{db_key}' ({db_type}): {e}")
        traceback.print_exc()
        return None


import pandas as pd
# from your_other_module import insert_table_black   # <-- Import as needed

def query_and_print_db_space(conn,db_key):
   while True:
       while True:
        print("Select database to query:")
        if db_key=='TDPROD':
            input_names=input("enter the db name in comma separated or line separated ").upper()
        else:
            input_names = input(f"Enter choice \n       1)BQSTAGE_TS1 \n       2)BQSTAGE_TS3 \n       else enter the db name in comma separated or line separated ").upper()
            if input_names == "1":
                input_names = "bqstage_ts1\n"
            elif input_names == "2":
                input_names = "bqstage_ts3\n"

        database_list=input_names.replace(',','\n').splitlines()
        database_names=",".join(f"'{item.strip()}'" for item in database_list if item.strip())


        valid_qry=f""" select count(1)
            FROM
            DBC.DatabasesV
            where
            databasename IN ({database_names})
            """
        log_message(f"[DEBUG] Validation Query \n{valid_qry}")
        df = pd.read_sql(valid_qry, conn)
        log_message(f"[INFO] Validation Dataframe count:{len(df)}")
        if df.iloc[0,0] == 0:
            ans=input("No Valid Databases found for the inputs, please press press [y] to Repeat [n] to exit").strip().lower()
            if ans=="y":
                continue
            else:
                return;

        log_message(f"[INFO] Framing size of the DB Query")
        query = f"""
        SELECT
            DatabaseName,
            SUM(CurrentPerm)/1024/1024/1024 AS USEDSPACE_IN_GB,
            SUM(MaxPerm)/1024/1024/1024 AS MAXSPACE_IN_GB,
            CASE 
                WHEN SUM(MaxPerm) = 0 THEN 0 
                ELSE (SUM(CurrentPerm)/SUM(MaxPerm))*100 
            END AS Percentage_Used,
            (SUM(MaxPerm)/1024/1024/1024) - (SUM(CurrentPerm)/1024/1024/1024) AS REMAININGSPACE_IN_GB
        FROM DBC.DiskSpace
        WHERE DatabaseName IN ({database_names})
        GROUP BY DatabaseName
        ORDER BY 3 DESC;
        """
        log_message(f"[INFO] Executing DB size Query")
        df = pd.read_sql(query, conn)
        insert_table_black(df)  # Uses the imported function
        if input_names == "bqstage_ts1\n":
            if input("Would like to clear the space and wants to have the truncate commnand [y] Yes").lower().strip()=="y":
                qry=f"""
                SELECT
                 t.DatabaseName AS schema_name,
                 t.TableName AS table_name,
                 ROUND(SUM(t.CurrentPerm)/(1024*1024),2) AS mb,
                 ROUND(SUM(t.CurrentPerm)/(1024*1024*1024),3) AS gb,
                 'DELETE FROM '||TRIM(t.DatabaseName)||'.'||TRIM(t.TableName)||' ALL;' AS delete_sql
                FROM DBC.TableSizeV t
                WHERE t.DatabaseName='BQSTAGE_TS1'
                GROUP BY t.DatabaseName,t.TableName
                HAVING ROUND(SUM(t.CurrentPerm)/(1024*1024*1024),3) >= 1
                ORDER BY gb DESC;
                    """
                delete_df = pd.read_sql(qry, conn)
                trunc_qry_list = delete_df["delete_sql"].astype(str).tolist()
                insert_table_black(delete_df)
                trunc_qry="\n".join(trunc_qry_list)
                copy_friendly_print(trunc_qry)
        ans=input("Would you like to check for any other DB's [y] yes or [n] No").strip().lower()
        if ans=="y":
           continue
        elif ans=="n":
            return
        else:
            print("Invalid input Returning to main menu")
            return

def build_union_all_query(df,ing_type):
    """
    Generates a UNION ALL SQL query to get table sizes for each (schema, table)
    in the DataFrame. Assumes columns are already in uppercase.
    """
    query_list = []
    if ing_type=='FI':
        for _, row in df.iterrows():
            schema = row['SOURCE_SCHEMA']
            table = row['SOURCE_TABLE_NAME']
            sql = f"""
                SELECT
                t.DatabaseName  AS schemaName,
                t.TableName     AS tableName,
                ROUND(SUM(t.CurrentPerm) / (1024*1024), 2)     AS sizeInMB,
                ROUND(SUM(t.CurrentPerm) / (1024*1024*1024), 2) AS sizeInGB,
                ROUND(SUM(t.CurrentPerm) / (1024*1024*1024*1024), 2) AS sizeInTB
            FROM
                DBC.TableSizeV t
            WHERE
                t.DatabaseName = '{schema}'
              AND t.TableName  = '{table}'
            GROUP BY
                t.DatabaseName, t.TableName
            """

            query_list.append(sql.strip())
    else:
        for _, row in df.iterrows():
            project_id=row['SOURCE_DB_NAME']
            dataset = row['SOURCE_SCHEMA']
            table = row['SOURCE_TABLE_NAME']
            sql=f"""SELECT
                  table_schema AS dataset_name,
                  table_name,
                  total_rows,
                  ROUND(total_physical_bytes/POW(1024,2), 2)  AS size_MB,
                  ROUND(total_physical_bytes/POW(1024,3), 2)  AS size_GB,
                  ROUND(total_physical_bytes/POW(1024,4), 2)  AS size_TB
                FROM
                  `{project_id}`.`region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
                WHERE
                  table_schema = '{dataset}'            
                  AND table_name = '{table}'
                """
            query_list.append(sql.strip())

    # Join all individual queries with UNION ALL
    return "\nUNION ALL\n".join(query_list)

# Example usage:
# result_query = build_union_all_query(df)
# print(result_query)
# result_df = pd.read_sql(result_query, teradata_conn)


def source_drift(creds, env_choice):
    table_name = input(f"{INDENT} Please enter the table name to be synced: ").strip().upper()
    if not table_name:
        print("❌ Table name cannot be empty")
        return
    print(f"✅ Table you entered: {table_name}")

    # environment selection
    if env_choice == "1":
        conn = get_dynamic_db_connection("EJCRO", creds)
        env_name='PRD'
    else:
        env_sub_choice = input(f"{INDENT} Choose sub-env (1=TS1, 2=TS3): ").strip()
        if env_sub_choice == "1":
            conn = get_dynamic_db_connection("EJCTS1", creds)
            env_name = 'TS1'
        elif env_sub_choice == "2":
            conn = get_dynamic_db_connection("EJCTS3", creds)
            env_name = 'TS3'
        else:
            print("❌ Invalid choice")
            return

    with conn.cursor() as cur:
        # Call Oracle function
        result = cur.callfunc("get_cmds", cx_Oracle.STRING, [table_name])
        df = pd.DataFrame({"OUTPUT": [result]})
        print("\n🔹 Commands generated for processing:")
        copy_friendly_print(result)
        print("Now,lets compare metadata")
        compare_metadata(env_name,table_name)

        # Ask for backup
        if input("Would you like me to create a snapshot for backup (y/n)? ").strip().lower() != "y":
            return

        # Query job_streams
        date_suffix = datetime.now().strftime("%d%m%Y")
        fqn_qry = f"""
            SELECT TARGET_DB_NAME || '.' || TARGET_SCHEMA || '.' || TARGET_TABLE_NAME AS FQN
            FROM bq_job_streams
            WHERE job_stream_id LIKE '%BQ'
              AND active_ind = 'Y'
              AND workflow_type <> 'SRC2STG'
              AND target_table_name = '{table_name}'
        """
        cur.execute(fqn_qry)
        rows = cur.fetchall()
        col_names=[desc[0] for desc in cur.description]
        df=pd.DataFrame(rows, columns=col_names)
        value=df.iloc[0]
        print(f"full qualified  table name {value[0]}")
        str=value[0]
        result = str.rsplit(".", 1)[0]
        print(f"result:{result}")
        if len(rows) == 0:
            print("⚠️ No rows found in bq_job_streams for the entered table")
            return
        elif len(rows) > 1:
            print("⚠️ Multiple rows returned, please check manually")
            for r in rows:
                print(r)
            return
        value = rows[0][0]

    # Decide dataset prefix
    if env_choice == "1":
        dataset_prefix = "gcp-edwprddata-prd-33200.WORK_OPERATIONSDB"
    elif env_sub_choice == "1":
        dataset_prefix = "gcp-edwstgdata-nprd-84293.WORK_OPERATIONSDB_TS1"
    else:
        dataset_prefix = "gcp-edwstgdata-nprd-84293.WORK_OPERATIONSDB_TS3"

    back_up_script = f"""
    CREATE SNAPSHOT TABLE {dataset_prefix}.{table_name}_{date_suffix}
    CLONE {value}
    OPTIONS (
      expiration_timestamp = DATE_ADD(current_timestamp(), INTERVAL 90 DAY)
    );
    """
    print("\n📂 Backup script:")
    copy_friendly_print(back_up_script)

    if input("Would you like me to create a insert script for new table (y/n)? ").strip().lower() != "y":
        return
    if env_choice == "1":
        client=bigquery.Client.from_service_account_json("BQ_PRD_Key.json")
    else:
        client = bigquery.Client.from_service_account_json("BQ_STG_Key.json")
    tab_cols=f""" SELECT STRING_AGG(column_name, ', ' ORDER BY ordinal_position) AS column_names
            FROM `{result}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'"""
    query_result=client.query(tab_cols)
    # print(f"INFO: query to find table columns")
    # copy_friendly_print(tab_cols)

    df_cols=query_result.to_dataframe(query_result)
    df_cols=df_cols.iloc[0,0]
    print(f"INFO: table columns")
    try:
        usr_input=input(f"Enter the table name the bkp table name, else enter n to take the default table ").strip().upper()
        bkp_tbl_nm = f"{table_name}_{date_suffix}" if usr_input == "N" else usr_input

        back_up_tab_cols = f""" SELECT STRING_AGG(column_name, ', ' ORDER BY ordinal_position) AS column_names
                FROM `{dataset_prefix}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{bkp_tbl_nm}'"""
        # print(f"INFO: back up table fetching script \n {back_up_tab_cols}")
        qry_res = client.query(back_up_tab_cols)
        df_cols1=qry_res.to_dataframe(qry_res)
        df_cols1=df_cols1.iloc[0,0]
    except Exception as e:
        log_message(f"[ERROR] back up table not found , hence using the actual table columns - {e}")
        df_cols1=df_cols

    ins_script=f""" insert into {str} ({df_cols}) \n select {df_cols1} from {dataset_prefix}.{bkp_tbl_nm}"""
    copy_friendly_print(ins_script)


def others_utilities(creds):

  while True:
    print("Other Utilities:")
    print("1. Query disk for tables")
    print("2. Source Drift")
    print("3. Get Fully Qualified Table Names")
    print("4. JCT,Metadata and Merge Key  Related")
    print("5. Excel Utility & Promotion")
    print("6. Failed,Running and Tables Associated with Job Group ")
    print("0. Go To Main Menu")
    # (Add more options here later)

    choice = input("Enter option number  ").strip()

    if choice == "1" or choice == "3":
        if choice == "1":
            print("1. Query disk space in Teradata DB wise")
            print("2. Find the size of the table")
            inp=input("Enter the choice").strip()
            print("1. PROD")
            print("2. NON-PROD")
            env = input("Enter the environment ").strip()


            if inp=="1":
                db_key = 'TDPROD' if env == '1' else 'TDTEST'
                conn = get_dynamic_db_connection(db_key, creds)
                query_and_print_db_space(conn,db_key)
                conn.close()
                continue

            else:
                from parity_analysis import get_job_streams_for_tables
                input_ing_type = input(f"Select the Ingestion Type \n       1) FI  \n       2) RI")
                if input_ing_type == "1":
                    db_key = 'TDPROD' if env == '1' else 'TDTEST'

                if env=="1":
                    dname = 'EJCRO'
                elif env=="2":
                    print('\n       1)TS1\n       2)TS3')
                    sub_choice = input(f"Enter Choice:\n").strip()
                    if sub_choice == "1":
                        dname = 'EJCTS1'
                    elif sub_choice == "2":
                        dname = 'EJCTS3'
                    else:
                        print("invalid choice")
                        return
                else:
                    print("invalid choice")
                    return
            input_tables = input(f"{INDENT}Enter target_table_names (comma or newline separated):\n").strip()
            table_list = [tbl.strip().upper() for tbl in input_tables.replace(",", "\n").splitlines() if
                          tbl.strip()]
            if not table_list:
                print(f"{INDENT}❌ No table names provided.")
                return
            creds = load_db_credentials()
            conn = get_dynamic_db_connection(dname, creds)

            if input_ing_type=="1":

                ing_type='FI'
            elif  input_ing_type=="2":
                ing_type = 'RI'
            conn1 = None  # declare before the loop
            while True:
                df_job_streams = get_job_streams_for_tables(conn, table_list, ing_type)

                # Usage
                log_message(f"[INFO] Filtering  from job_streams to fetch only SRC2STG rows ")
                df_filtered = df_job_streams[df_job_streams['WORKFLOW_TYPE'] == 'SRC2STG']
                log_message(f"[INFO] Building Union All Query for the Given tables ")
                union_query = build_union_all_query(df_filtered,ing_type)
                # log_message(f"[DEBUG] Union Query \n {union_query}")
                src_db = df_filtered['SOURCE_DB_NAME'].iloc[0]
                if ing_type=='RI':
                    from parity_analysis import get_bq_client
                    if env=="1":
                        client=get_bq_client('PROD')
                    else:
                        client = get_bq_client('NON-PROD')
                if ing_type=='FI':
                    if conn1 is  None:
                     conn1 = get_dynamic_db_connection(src_db, creds)
                    result_df = pd.read_sql(union_query, conn1)
                else:
                    # log_message(f"[INFO] Framed Union ALl Query \n     {union_query}")
                    result_df = client.query(union_query).to_dataframe()

                result_df.insert(0,"SNO",range(1,len(result_df)+1))
                insert_table_black(result_df)
                if len(result_df)>1:
                    ans=input("Would you like to printed in order[y] Yes or [n] No").lower()
                    if ans=="y":
                        result_df.columns = [c.lower() for c in result_df.columns]
                        log_message(f"[INFO] columns names:{result_df.columns}")
                        if ing_type=="RI":
                            sorted_df = result_df.sort_values(by="size_mb").reset_index(drop=True)
                        else:
                            sorted_df = result_df.sort_values(by="sizeinmb").reset_index(drop=True)
                        sorted_df["sno"] = range(1, len(sorted_df) + 1)
                        insert_table_black(sorted_df)
                ans1=input("Would you like to check for any other tables [y] Yes or [n] No").lower()
                log_message(f"[INFO] Answer: {ans1}")
                if ans1=="y":
                    input_tables = input(f"{INDENT}Enter target_table_names (comma or newline separated):\n").strip()
                    table_list = [tbl.strip().upper() for tbl in input_tables.replace(",", "\n").splitlines() if
                                  tbl.strip()]
                    continue
                else:
                    break
        else:
            print("1. PROD")
            print("2. NON-PROD")
            env = input("Enter the environment ").strip()

            if env == "1" or env == "2":
                if env == "2":
                    print("Select the option")
                    print('\n 1 For TS1\n 2 For TS3')

                    sub_choice = input(f"{INDENT}Enter Choice:\n").strip()
                    if sub_choice == "1":
                        dname = 'EJCTS1'
                    elif sub_choice == "2":
                        dname = 'EJCTS3'
                    else:
                        print("invalid choice")
                        return
                else:
                    dname = 'EJCRO'

                input_tables = input(f"{INDENT}Enter target_table_names (comma or newline separated):\n").strip()
                table_list = [tbl.strip().upper() for tbl in input_tables.replace(",", "\n").splitlines() if tbl.strip()]
                if not table_list:
                    print(f"{INDENT}❌ No table names provided.")
                    return

                creds = load_db_credentials()
                conn = get_dynamic_db_connection(dname, creds)
                if choice=="2":
                    from parity_analysis import get_job_streams_for_tables
                    df_job_streams = get_job_streams_for_tables(conn, table_list, 'FI')

                    # Usage
                    df_filtered = df_job_streams[df_job_streams['WORKFLOW_TYPE'] == 'SRC2STG']
                    union_query = build_union_all_query(df_filtered)
                    src_db = df_filtered['SOURCE_DB_NAME'].iloc[0]
                    conn = get_dynamic_db_connection(src_db, creds)
                    # Execute the query (assuming you have a Teradata connection as `td_conn`)
                    result_df = pd.read_sql(union_query, conn)
                    print("\n")
                    insert_table_black(result_df)
                    ans=print("Would like to have it printed in the order [y] Yes or [n] No").lower()
                    if ans=="y":
                        result_df.columns = [c.lower() for c in result_df.columns]
                        sorted_df=result_df.sort_values(by="sizeinmb")
                        insert_table_black(sorted_df)
                else:
                    placeholders = ", ".join([f"'{tbl}'" for tbl in table_list])
                    # Build the dynamic SQL query
                    query = f"""
                        SELECT 
                            decode(fil, 'TD','RI','BQ','FI') AS ingestion_type,
                            MAX(CASE 
                                WHEN workflow_type = 'SRC2STG' AND fil = 'TD' 
                                THEN SOURCE_DB_NAME || '.' || SOURCE_SCHEMA || '.' || SOURCE_TABLE_NAME
                                WHEN workflow_type = 'SRC2STG' AND fil = 'BQ' 
                                THEN SOURCE_SCHEMA || '.' || SOURCE_TABLE_NAME
                            END) AS src_table_fqn,
                            MAX(CASE 
                                WHEN workflow_type <> 'SRC2STG' AND fil = 'TD' 
                                THEN TARGET_SCHEMA || '.' || TARGET_TABLE_NAME
                                WHEN workflow_type <> 'SRC2STG' AND fil = 'BQ' 
                                THEN TARGET_DB_NAME || '.' || TARGET_SCHEMA || '.' || TARGET_TABLE_NAME
                            END) AS tgt_table_fqn
                        FROM (
                            SELECT 
                                substr(job_stream_id, -2) AS fil, 
                                t.* 
                            FROM bq_job_streams t 
                            WHERE active_ind = 'Y' 
                              AND target_table_name IN ({placeholders})
                        ) q
                        GROUP BY decode(fil, 'TD','RI','BQ','FI'), target_table_name
                        ORDER BY ingestion_type, target_table_name
                        """
                        # Execute query and fetch into DataFrame
                    df = pd.read_sql(query, conn)
                    insert_table_black(df)

                    if df["INGESTION_TYPE"].nunique() != 1:
                        user_input=input("Enter, FI only [f], RI only [r]").lower()
                        if user_input =="r":
                            df=df[df["INGESTION_TYPE"]=='RI']
                            insert_table_black(df)
                        elif  user_input =="f":
                            df=df[df["INGESTION_TYPE"]=='FI']
                            insert_table_black(df)
                        else:
                            print("invalid input")
                            return

                    comma_choice=input("would you like Comma separated names of the tables? y or n").lower()
                    if comma_choice=="y":
                        names = "Source tables:" +"\n" + ", ".join(df["SRC_TABLE_FQN"].astype(str))
                        names1= "Target tables:" +"\n" + ", ".join(df["TGT_TABLE_FQN"].astype(str))
                        combined_name=f"{names}\n{names1}"
                        copy_friendly_print(combined_name)
                    src_tgt_choice=input("Do you need only Target tables or Source tables [t] Target [s] source").lower()
                    if src_tgt_choice=="t":
                        names1=names1.split('\n', 1)[1]
                        copy_friendly_print(names1)
                    elif   src_tgt_choice=="s":
                        names = names.split('\n', 1)[1]
                        copy_friendly_print(names)
                    else:
                        print("Invalid choice")

            else:
                print("\nYou have selected an invalid choice")
    elif choice == "2" :
        print(f"{INDENT} 1)PROD")
        print(f"{INDENT} 2)NON-PROD")

        while True:
            env_choice = input(f"{INDENT} Enter your Choice (1 or 2, or 'exit' to go back): ").strip().lower()

            if env_choice == "exit":
                print(f"{INDENT} Exiting submenu...")
                return
            elif env_choice == "1" or env_choice == "2":
                source_drift(creds,env_choice);
                break
            else:
                print(f"{INDENT} Invalid choice. Please enter 1, 2, or 'exit'.")
    elif choice == "4" :
          print(f"{INDENT} 1) JCT Update Scripts")
          print(f"{INDENT} 2) Metadata ")
          print(f"{INDENT} 3) Add Merge Keys Script ")
          sub_choice=input("Enter The Choice").strip()
          if sub_choice=="1":
              print(f"""Select the below options \n        1)One Time Full Load Script \n        2)Set Run_status to 'P'
        3)Set incremental column names \n        4)Backdate i.e update to_extract_dtm 
        5)Set Active_ind"""  )
              jct_choice=input("Eneter the choice").strip().lower()
              if jct_choice=="1":
                  full_load_Setup(creds,run_status=None)
              elif  jct_choice=="2":
                  set_status_to_p(creds)
              elif jct_choice in ["3","4",'5']:
                  update_bq_job_streams(jct_choice)
              else:
                  print("Invalid choice")
                  break
          elif  sub_choice=="2":
               display_metadata(creds)
          elif sub_choice=="3":
               generate_merge_key(creds)
          else:
              print("Invalid Choice")
    elif choice=="5":
        print(f"{INDENT}1)Excel Utilities")
        print(f"{INDENT}2)Promotion")
        sub_choice=input("Enter your choice").strip().lower()
        if sub_choice=="1":
            print(f"{INDENT}1)Give Comma separated values")
            print(f"{INDENT}2)Put Quote on each item separated by comma")
            ans=input("your choice").strip().lower()
            if ans=="1":
                input_items=input("Enter list of items ").upper()
                split_items=input_items.replace(" ",'\n').splitlines()
                comma_sep_items=",".join([item for item in split_items if item.strip()])
                copy_friendly_print(comma_sep_items)
            elif  ans=="2":
                input_items=input("Enter list of items ").upper()
                split_items=input_items.replace(" ",'\n').splitlines()
                quoted_items="(" + ",".join([f"'{item}'" for item in split_items if item.strip()] ) + ")"
                copy_friendly_print(quoted_items)

        elif sub_choice=="2":
           Job_promotion_automation.main()
    elif choice == "6":
        print(" \n        1)Currently Running \n        2)Currently Failed \n        3)Tables Associated with a job")
        sub_choice=input("Enter The Choice").strip().lower()
        env_choice=input("Enter The Choice\n        1)PRD\n        2)TS1\n        3)TS3").lower().strip()
        if env_choice=="1":
            env='PRD'
            db_key='EJCRO'

        elif env_choice=="2":
            env='TS1'
            db_key='EJCTS1'

        elif env_choice=="3":
            env='TS3'
            db_key='EJCTS3'
            # list_tables_with_job_group(env, db_key)
        if    sub_choice=="1":
            currently_running_jobs(env, db_key)
        elif  sub_choice=="2":
            recently_failed_jobs(env, db_key)

    elif  choice == "0" :
          break
    else:
        print("Invalid choice or feature not implemented yet.")



def display_bq_job_streams_table(creds):
    INDENT = "    "

    # Step 1: Environment Selection (Numbered Menu)
    print(f"{INDENT}Select environment to connect:")
    print(f"{INDENT}1) PROD")
    print(f"{INDENT}2) NON-PROD")
    env_choice = input(f"{INDENT}Enter choice (1 or 2): ").strip()

    if env_choice == "1":
        db_key = "EJCRO"
    elif env_choice == "2":
        # Non-prod, show TS1/TS3 choice
        print(f"{INDENT}Select NON-PROD instance:")
        print(f"{INDENT}1) TS1")
        print(f"{INDENT}2) TS3")
        ts_choice = input(f"{INDENT}Enter choice (1 or 2): ").strip()
        if ts_choice == "1":
            db_key = "EJCTS1"
        elif ts_choice == "2":
            db_key = "EJCTS3"
        else:
            print(f"{INDENT}Invalid choice. Must be 1 or 2.")
            return
    else:
        print(f"{INDENT}Invalid choice. Must be 1 or 2.")
        return

    # Step 2: Workflow Selection (Numbered Menu)
    print(f"{INDENT}Select workflow type:")
    print(f"{INDENT}1) FI")
    print(f"{INDENT}2) RI")
    workflow_choice = input(f"{INDENT}Enter choice (1 or 2): ").strip()
    if workflow_choice == "1":
        workflow_type = "FI"
        job_stream_filter = "job_stream_id LIKE '%BQ'"
    elif workflow_choice == "2":
        workflow_type = "RI"
        job_stream_filter = "job_stream_id LIKE '%TD'"
    else:
        print(f"{INDENT}Invalid workflow choice. Must be 1 or 2.")
        return

    # Step 3: Accept Table Name(s)
    input_tables = input(f"{INDENT}Enter target_table_names (comma or newline separated):\n").strip()
    table_names = [tbl.strip().upper() for tbl in input_tables.replace(",", "\n").splitlines() if tbl.strip()]
    if not table_names:
        print(f"{INDENT}No table names entered.")
        return

    print(f"{INDENT}Connecting to Oracle ({db_key})...")

    try:
        with get_dynamic_db_connection(db_key, creds) as conn:
            with conn.cursor() as cursor:

                table_names_sql = ', '.join(f"'{name}'" for name in table_names)
                query = f"""
                    SELECT * FROM bq_job_streams
                    WHERE target_table_name IN ({table_names_sql})
                      AND active_ind = 'Y'
                      AND {job_stream_filter}
                """
                cursor.execute(query)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(rows, columns=columns)
                if not df.empty:
                    df['WORKFLOW_PRIORITY'] = (df['WORKFLOW_TYPE'].str.upper() != 'SRC2STG').astype(int)
                    df = df.sort_values(['TARGET_TABLE_NAME', 'WORKFLOW_PRIORITY'])
                    df = df.drop(columns=['WORKFLOW_PRIORITY'])
                    # Now display or use the sorted df
                    df.insert(0,"Sno",range(1,len(df)+1))
                    insert_table_black(df)  # or print(df)
                    print(f"[r] Refresh,[s] src2stg rows")
                    ans=input()
                    if ans=='s':
                        df=df[df["WORKFLOW_TYPE"]=='SRC2STG']
                        df["Sno"]=range(1,len(df)+1)
                        insert_table_black(df)

                else:
                    print(f"{INDENT}No data found for any of the given tables.")



    except Exception as e:
        import traceback
        log_message(f"{INDENT}Error occurred:\n{traceback.format_exc()}")

