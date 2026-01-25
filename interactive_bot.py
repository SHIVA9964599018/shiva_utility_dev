# interactive_bot.py

import oracledb as cx_Oracle
cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\shivac\Downloads\instantclient-basic-windows.x64-23.8.0.25.04\instantclient_23_8")

import pandas as pd
import json
import teradatasql
from google.cloud import bigquery
import textwrap

from utils import *
from count import *
from chat_ui import *
from job_status import check_run_status

from parity_analysis import run_parity_analysis  # NEW: Parity logic handler
from general_menu import general_menu
import ast
from shortcut import short_cuts

with open('db_cred.json') as f:
    creds = json.load(f)

INDENT = " " * 10



# Option 1: Ingestion logic

def ingestion_menu(creds):
    print(f"\n{INDENT}Ingestion Options:")
    print(f"{INDENT}1) PROD")
    print(f"{INDENT}2) NON-PROD")

    choice = input(f"{INDENT}Enter your ingestion option: ").strip()
    actions = {"1": "PROD", "2": "NON-PROD"}

    # --- Environment selection ---
    if choice == "2":
        print(f"Please select which environment")
        print(f"{INDENT}1) TS1")
        print(f"{INDENT}2) TS3")
        env = input(f"{INDENT}Enter Your Choice: ").strip()
        print(f"You Selected {env}")

        if env == "1":
            db_key = "EJCTS1"
        elif env == "2":
            db_key = "EJCTS3"
        else:
            print("Invalid environment selection.")
            return
    elif choice == "1":
        db_key = "EJCRO"
    else:
        print("Invalid ingestion option.")
        return

    # --- Table input ---
    input_tables = input(f"Enter table names you want to ingest: ").strip().upper()
    table_names = input_tables.replace(",", "\n").splitlines()
    table_names = [x.strip() for x in table_names if x.strip()]
    table_names = "(" + ",".join(f"'{table}'" for table in table_names) + ")"
    print(f"[INFO] Tables selected: {table_names}")


    # --- Workflow type selection ---
    print("Select workflow type:")
    print(f"{INDENT}1) FI")
    print(f"{INDENT}2) RI")
    wf_type = input("Please select the option: ").strip()

    if wf_type == "1":
        job_stream_filter = " job_stream_id like '%BQ' "
    elif wf_type == "2":
        job_stream_filter = " job_stream_id like '%TD' "
    else:
        print("Invalid workflow type.")
        return

    # --- DB Connection ---
    with get_dynamic_db_connection(db_key, creds) as conn:
        if not conn:
            print(f"{INDENT}Could not connect to DB '{db_key}'")
            return

        with conn.cursor() as cursor:
            query = f"""
                SELECT * FROM bq_job_streams 
                WHERE target_table_name IN {table_names}
                  AND active_ind = 'Y'
                  AND {job_stream_filter}
                ORDER BY target_table_name,
                         CASE WHEN workflow_type='SRC2STG' THEN 1 ELSE 2 END
            """
            print(f"[INFO] Query: {query}")
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)

            if df.empty:
                print(f"{INDENT}No data found for any of the given tables.")
                return

            # --- Sort and display ---
            df['WORKFLOW_PRIORITY'] = (df['WORKFLOW_TYPE'].str.upper() != 'SRC2STG').astype(int)
            df = df.sort_values(['TARGET_TABLE_NAME', 'WORKFLOW_PRIORITY']).drop(columns=['WORKFLOW_PRIORITY'])
            df.insert(0, 'SNO', range(1, len(df) + 1))

            insert_table_black(df)
            print('Would you like to prepare SQL query to update the run_status to P?')
            answer = input("Enter Yes or No (y/n): ").strip().lower()

            if answer == 'y':
                if choice == "1":
                    # Ensure table_names string is a proper tuple, even for single table
                    if table_names.count("'") == 2:  # means only one table inside parentheses
                        table_names = table_names.replace("')", "',)")  # add comma before closing
                    tuple_data = ast.literal_eval(table_names)
                    table_list = list(tuple_data)
                    print(f"[INFO] Table list: {table_list}")
                    proc_bloc = []

                    # --- Generate PL/SQL update blocks per table ---
                    sno=0
                    for table_name in table_list:
                        qry = f"""
                            SELECT * 
                            FROM bq_job_streams 
                            WHERE target_table_name = '{table_name}'
                              AND {job_stream_filter} 
                              AND active_ind='Y'
                        """
                        df_table = pd.read_sql(qry, conn)

                        if df_table.empty:
                            print(f"❌ No matching entry found for table '{table_name}'")
                            continue

                        for _, row in df_table.iterrows():
                            js_id = row["JOB_STREAM_ID"]
                            wf_type_row = row["WORKFLOW_TYPE"]
                            sno=sno+1
                            block = f""" --{sno}
                                EDS_DATA_CATALOG.xxedw_ingestion_utl_BQ.updateJCT(
                                    p_exec_env=>'PRD',
                                    p_job_stream_id=>'{js_id}',
                                    p_run_status=>'P',
                                    p_comments=>'JCT Adjustment for job group'
                                );
                                                        """
                            proc_bloc.append(block)

                    if proc_bloc:
                        final_block = "BEGIN \n" + "\n".join(proc_bloc) + "\nEND;"
                        # print(final_block)
                        copy_friendly_print(final_block)
                        if input(f"Would you like to execute the above PLSQL Block in {db_key}").lower().strip() == "y":

                            user_choice = ask_confirmation(
                                "Would you like to execute the above PLSQL block?",
                                title="Confirm Execution"
                            )

                            if user_choice == 'Y':
                                try:
                                    cursor.execute(final_block)
                                    conn.commit()
                                    print("✅ PL/SQL block executed successfully.")
                                except Exception as e:
                                    print("❌ Error executing block:", e)
                            else:
                                print(f"Since user do not want the block to be Executed, going to next steps")

                    else:
                        print("[WARN] No PL/SQL blocks generated — no matching job_stream entries found.")

                else:
                    #this below variable is for divind table names in the where clause to appear in the screen
                    chunk_size1 = 4

                    # Remove surrounding parentheses and split into items
                    items = table_names.strip("()").replace("'", "").split(",")

                    # Add quotes back around each item and join in chunks
                    wrapped = "(" + "\n".join(
                        ", ".join(f"'{item}'" for item in items[i:i + chunk_size1]) + (
                            "," if i + chunk_size1 < len(items) else "")
                        for i in range(0, len(items), chunk_size1)
                    ) + ")"

                    qry = f"""
    UPDATE bq_job_streams 
    SET run_status='P' 
    WHERE target_table_name IN {wrapped} AND {job_stream_filter}
      AND active_ind='Y'
                         """
                    copy_friendly_print(qry)
                    if input(f"Would you like to execute the above Query in {db_key}").lower().strip()=="y":
                        user_choice = ask_confirmation(
                            f"Are you sure you want to execute above SQL query in {db_key}?",
                            title="Confirm Execution"
                        )

                        if user_choice == 'Y':
                            try:
                                cursor.execute(qry)
                                conn.commit()
                                print("✅ Query executed successfully.")
                            except Exception as e:
                                print("❌ Error executing Query:", e)
                        else:
                            print(f"Since user do not want the block to be Executed, Going to next steps")
            if input("Would you like to refresh the BQ_JOB_STREAM_TABLE For the tables [y] Yes [n] No").lower()=="y":
                cursor.execute(query)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(rows, columns=columns)
                insert_table_black(df)
            # --- Get JCT commands ---
            print('Would you like to get the JCT commands to run?')
            answer = input("Enter Yes or No (y/n): ").strip().lower()

            if answer == 'y':
                tnames = table_names.strip("()").replace("'", "")
                print(f"List of Tables \n {tnames} ")
                if wf_type == "1":
                    print(f"[DEBUG] calling get_jct for FI")
                    value = cursor.callfunc("get_jct", str, [tnames])
                else:
                    print(f"[DEBUG] calling get_jct for RI")
                    value = cursor.callfunc("get_jct_ri", str, [tnames])
                copy_friendly_print(value)
            cmd_list=value.splitlines()
            if len(cmd_list) >1:
                try:
                    chunk_size= int(input("Would you like commands to run in a batch of 3,4,5 etc if Yes enter the size of the batch").strip())

                except ValueError:
                    print(" Since user did not enter numeric value returning.")
                    return
            f_cmds = []
            if len(cmd_list) !=1:
                for i in range(0, len(cmd_list), chunk_size):
                    cmd = cmd_list[i:i + chunk_size]
                    f_cmds.append("\n".join(cmd))  # appends the combined string
                final = "nohup bash -c '\n" +  "\nwait\n".join(f_cmds) + "\n ' &"
                # print(final)
                copy_friendly_print(final)


# Main menu loop
def main_menu():
    while True:
        print(f"\n{INDENT}Please Select the Below Option")
        print(f"{INDENT}1) Main Menu")
        print(f"{INDENT}2) General")
        print(f"{INDENT}3) Others")
        print(f"{INDENT}4) Short-cuts")
        # print(f"{INDENT}4) Test Something")
        print(f"{INDENT}0) Exit")
        # print(f"{INDENT}8) MISC")  # 🔶 NEW menu option
        choice = input(f"{INDENT}Enter your choice: ").strip()

        try:
            if choice == "1":
                print(f"{INDENT}1) Ingestion")
                print(f"{INDENT}2) Finding count from Parity Audit Table")
                print(f"{INDENT}3) Check the run_status of a job")
                print(f"{INDENT}4) Check the bq_job_streams and display the table contents")
                print(f"{INDENT}5) Parity Analysis")  # New menu option
                print(f"{INDENT}0) Go to main Menu")  # going to main menu
                sub_choice = input(f"{INDENT}Enter your choice: ").strip()

                if sub_choice == "1":
                    ingestion_menu(creds)
                elif sub_choice == "2":
                    print_parity_validation_data()
                elif sub_choice == "3":
                    check_run_status()
                elif sub_choice == "4":
                    display_bq_job_streams_table(creds)
                elif sub_choice == "5":
                    run_parity_analysis()
                elif sub_choice == "0":
                    continue
                else:
                    print(f"{INDENT}Invalid choice. Returning to main menu...")

            elif choice == "2":
                general_menu(creds)
            elif choice == "3":
                others_utilities(creds)
            elif choice == "4":
                short_cuts(creds)
            elif choice == "0":
                print(f"{INDENT}Goodbye!")
                break
            # elif choice=="4":
            #     from shiva import update_bq_job_streams
            #     update_bq_job_streams()
                # print("\nFinal tables returned:", tables)

            else:
                print(f"{INDENT}Invalid choice. Try again.")
        except Exception:
            import traceback
            print(f"{INDENT}An error occurred:\n{traceback.format_exc()}")
            print(f"{INDENT}Returning to main menu...")

# Launch the chatbot GUI
if __name__ == "__main__":
    run_chat_bot(main_menu)
