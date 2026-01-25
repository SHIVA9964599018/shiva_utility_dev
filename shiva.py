# shiva.py

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

# Load .env file from parent directory (adjust path as discussed earlier)
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path=env_path)
from utils import *

creds=load_db_credentials()
def get_table_list():
    tab_list = []
while True:
 tab_list=input("\nenter the table names in comma seprated or line separated\n").upper().replace(",","\n").splitlines()
 tab_list1=[item.strip() for item in tab_list if item.strip()]
 print(f"famated list\n{tab_list1}")
 tab_list2=",".join([f"'{item}'" for item in tab_list1])
 print(f"Entered tab list:\n {tab_list2}")
 enclosed_items="(" + tab_list2 + ")"
 print(f"enclosed items \n {enclosed_items}")
 if input("Would you like to try with different inputs\n").lower()=="n":
    break



def update_bq_job_streams():
 try:
    print("Enetr the environment")
    print("     1)PRD\n     \2)TS1\n     3)TS3")
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
    print("Select the column to be updated ")
    print(f"1)update INCREMENTAL_COLUMN_NAME")
    print(f"2)back date i.e update TO_EXTRACT_DTM")
    print(f"3)update ACTVE_IND")
    jct_col=input().strip()
    no_of_days_backdate = None
    if jct_col=="1":
        incr_col=input("Enter incremental column name").upper().strip()
        print(f"the incremntal column name given is \n {incr_col}")
        jct_col_update=f"p_incremental_column_name=>'{incr_col}',"

    elif jct_col=="2":
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
            print(f"JCT column update:=>{jct_col_update}")
    elif jct_col=="3":
        print("Would you like to be Disabled or Enabled")
        print("1)Enable")
        print("2)Disable")
        active_ind="Y" if input("Enter Choice").lower()=="1" else "N"
        jct_col_update=f"p_active_ind=>'{active_ind}',"


    ing_type=input("Enter ingestion type \n       1)FI\n       2)RI")
    job_stream_filter=" AND job_stream_id like '%TD'" if ing_type=="2" else " AND job_stream_id like '%BQ'"
    tname=input("Enter table name ").upper().strip()
    proc_bloc=[]
    with  get_dynamic_db_connection(db_key, creds) as conn:
        with conn.cursor() as cur:
            qry=f""" select * from bq_job_streams where target_table_name='{tname}' and active_ind='Y'
                {job_stream_filter}
                """
            df = query_to_dataframe(cur,qry)
            insert_table_black(df)
            for _, row in df.iterrows():
                js_id = row["JOB_STREAM_ID"]
                wf_type_row = row["WORKFLOW_TYPE"]
                if wf_type_row == 'SRC2STG' and jct_col in ["1", "2"]:
                         if no_of_days_backdate is not None:
                             parsed_date = row["TO_EXTRACT_DTM"] - timedelta(days=no_of_days_backdate)
                             jct_col_update = f"p_to_extract_dtm=>TO_DATE('{parsed_date}', 'YYYY-MM-DD HH24:MI:SS'),"
                         print("In if")
                         block = f""" 
    EDS_DATA_CATALOG.xxedw_ingestion_utl_BQ.updateJCT(
        p_exec_env=>'{env}',
        p_job_stream_id=>'{js_id}',
        {jct_col_update}
        p_comments=>'JCT Adjustment for job group'
    );
                                    """
                         proc_bloc.append(block)
                elif  jct_col not in ["1", "2"]:
                    print("In elsif")
                    block = f""" 
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
                if input("Would you like to execute the above PLSQL block [y] Yes or [n] No").lower().strip()=='y':
                    try:
                        cur.execute(final_block)
                        conn.commit()
                        print("✅ PL/SQL block executed successfully.")
                    except Exception as e:
                        print("❌ Error executing block:", e)
                else:
                    return
            if input("Would like to refresh the BQ_JOB_STREAM Table ").lower().strip()=="y":
                df = query_to_dataframe(cur, qry)
                insert_table_black(df)
            else:
                return



 except Exception as e:
     import traceback
     print(f"[ERROR] ): {e}")
     traceback.print_exc()
     return None
if __name__ == "__main__":
    get_table_list()