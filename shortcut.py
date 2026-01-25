from utils import *
import ast
import oracledb as cx_Oracle
cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\shivac\Downloads\instantclient-basic-windows.x64-23.8.0.25.04\instantclient_23_8")
from chat_ui import *
import traceback
import teradatasql
# CONSOLE_PRINT=False

def show_streams(conn):
    try:
       cur = conn.cursor()
       while True:
            print("1) PRD")
            print("2) TS1")
            print("0  Go Back")
            env_choice = input("Enter your choice (1/2/0): ").strip()
            if env_choice == "1":
                catalog_db = "EJCRO"
            elif env_choice == "2":
                catalog_db = "EJCTS1"
            elif env_choice == "0":
                break
            else:
                log_message(f"Inavalid input {env_choice}")
                return
            print("1)FI")
            print("2)RI")
            print("0)Go back")
            wkf_type = input("Enter your choice (1/2/0): ").strip()
            if wkf_type == "1":
                job_stream_filter = " AND JOB_STREAM_ID LIKE '%BQ'"
            elif wkf_type == "2":
                job_stream_filter = " AND JOB_STREAM_ID LIKE '%TD'"
            elif  wkf_type == "0":
                  continue
            else:
                log_message(f"Inavalid input {wkf_type}")
                return

            while True:
             input_table= input("enter the table names either comma separated or line separated").upper().strip()
             log_message(f"[INFO] input tables enetred ::{input_table}")
             input_tables=input_table.replace(",","\n")
             log_message(f"[INFO] After replacing comma with new line::{input_tables}")
             table_list=input_tables.splitlines()
             log_message(f"[INFO] After splitting the lines using split lines::{table_list}")
             final_tab_list="(" + ",".join([f"'{tname}'" for tname in table_list if tname.strip()] ) + ")"
             log_message(f"final_tab_list::{final_tab_list}")
             qry=f"""select * from {catalog_db}.bq_job_streams where target_table_name in {final_tab_list}
               {job_stream_filter} AND active_ind='Y'
                """
             log_message(f"job stream query::{qry}")

             df=query_to_dataframe(cur,qry)
             df.insert(0,"SNO",range(1,len(df) +1 ))
             insert_table_black(df)
             ans=input("only src2stg entries [s]").strip().lower()
             if ans=="s":
                   df1=df[df["WORKFLOW_TYPE"]=="SRC2STG"]
                   df1["SNO"]=range(1,len(df1) +1 )
                   insert_table_black(df1)
             else:
                 break
             if input("would like to continue for any other tables [y] yes ,[n] no").strip().lower()=="y":
                 continue
             else:
                 break

    finally:
        cur.close()
def parse_time_input(time_input: str):
    """Parses input like '1d', '2h', '15m' into Oracle interval filter."""
    time_input = time_input.strip().lower()
    if time_input.endswith("d"):
        try:
            days = int(time_input[:-1])
            return f"SYSDATE - {days}", f"{days} day(s)"
        except ValueError:
            pass
    elif time_input.endswith("h"):
        try:
            hours = int(time_input[:-1])
            return f"SYSDATE - ({hours}/24)", f"{hours} hour(s)"
        except ValueError:
            pass
    elif time_input.endswith("m"):
        try:
            minutes = int(time_input[:-1])
            return f"SYSDATE - ({minutes}/(24*60))", f"{minutes} minute(s)"
        except ValueError:
            pass

    # default fallback
    return "SYSDATE - 1", "1 day"
def run_status(conn):
   cursor=None
   try:
    cursor = conn.cursor()
    while True:
        print("1) PROD")
        print("2) NON-PROD")
        print("0) Go Back")
        env_choice = input("Enter your choice (1/2/0): ").strip()
        if env_choice == "1":
            catalog_db = "EJCRO"
        elif env_choice == "2":
            print("1) TS1")
            print("2) TS3")
            print("0) Go Back")
            print("9) Main Menu")
            env_sub_choice = input("Enter your choice (1/2/0): ").strip()
            if env_sub_choice == "1":
                catalog_db = "EJCTS1"
            elif env_sub_choice == "2":
                catalog_db = "EJCTS3"
            elif env_sub_choice == "0":
                continue
            elif env_sub_choice == "9":
                return
        elif env_choice == "0":
            return
        else:
            print(f"Invalid Choice returning back")
            return
        print(f"[INFO] env_choice::{env_choice}")
        print(f"[INFO] catalog_db::{catalog_db}")
        # Step 1: Get user input for type (FI/RI)
        while True:
            selected_choice = ""
            active_or_failed = 'N'
            completed = 'N'
            cnt = 'N'

            print(f"{INDENT}Please select the type:")
            print(f"{INDENT}1) FI")
            print(f"{INDENT}2) RI")
            print(f"{INDENT}0) Go Back")
            print(f"{INDENT}9) Main Menu")
            selected_choice = input(f"{INDENT}Enter your choice (1 or 2 or 0): ").strip()
            if selected_choice == "9":
                return

            if selected_choice not in ["1", "2"]:
                break

            # Step 2: Map the numerical choice to the corresponding type
            job_stream_filter_clause = ""
            if selected_choice == "1":
                job_stream_filter_clause = "AND job_stream_id LIKE '%BQ'"
            elif selected_choice == "2":
                job_stream_filter_clause = "AND job_stream_id LIKE '%TD'"
            while True:
                print(f"{INDENT}1) View latest records for table(s)")
                print(f"{INDENT}2) Check by ID of the person")
                print(f"{INDENT}0) Go Back")
                print(f"{INDENT}9) Main Menu")

                choice = input(f"{INDENT}Enter your choice (1 or 2 or 0): ").strip()
                if choice == "0":
                    break
                elif choice == "9":
                    return
                elif choice == "1":
                    # --- TABLE FLOW ---
                    input_tables = input(
                        f"{INDENT}Enter target_table_names (comma or newline separated):\n").strip()
                    table_list = [tbl.strip().upper() for tbl in input_tables.replace(",", "\n").splitlines() if
                                  tbl.strip()]
                    if not table_list:
                        raise ValueError("No tables provided.")
                    table_list_sql = ", ".join(f"'{tbl}'" for tbl in table_list)

                    iters_raw = input(
                        f"{INDENT}Show rows AFTER which iteration? Enter a positive integer (e.g., 1, 2, 5, 10): "
                    ).strip()
                    if not iters_raw.isdigit():
                        print("Iterations must be a positive integer (>= 1). Hence defaulting to 1")
                        num_runs="1"
                    num_runs=int(iters_raw)

                    description = f"{INDENT}Rows newer than the {num_runs}ᵗʰ SRC2STG run per table for: {', '.join(table_list)}"

                    while True:
                        query = f"""
                             WITH nth_run AS (
                               SELECT ttn, start_time
                               FROM (
                                 SELECT
                                   ttn,
                                   start_time,
                                   ROW_NUMBER() OVER (PARTITION BY ttn ORDER BY start_time DESC) AS rnk
                                 FROM {catalog_db}.stm_mst
                                 WHERE ttn IN ({table_list_sql})
                                 {job_stream_filter_clause}
                                   AND workflow_type = 'SRC2STG'
                               )
                               WHERE rnk = {num_runs}
                             )
                             SELECT a.*
                             FROM {catalog_db}.stm_mst a
                             JOIN nth_run b
                               ON a.ttn = b.ttn
                             WHERE a.start_time >= b.start_time
                               AND a.ttn IN ({table_list_sql})
                               {job_stream_filter_clause}
                             ORDER BY
                               a.ttn,
                               a.start_time DESC,
                               CASE WHEN a.workflow_type = 'SRC2STG' THEN 2 ELSE 1 END
                         """

                        df = query_to_dataframe(cursor, query)


                        print("\n" + description)
                        if completed == "y":
                            df = df[df["CURRENT_PHASE"] == "Success"]
                        elif active_or_failed == "y":
                            df = df[df["CURRENT_PHASE"] != "Success"]
                        df.insert(0, 'SNO', range(1, len(df) + 1))

                        if cnt == 'y':
                            df = df[df['WORKFLOW_TYPE'] != 'SRC2STG'][['TTN', 'TGT_ROWS']]
                        insert_table_black(df)
                        # show_dataframe(df, title="My Data Viewer", geometry="900x450", heading_bg="#3C8DBC", row_sep_color="#E0E6ED", show_row_separators=True, wheel_smooth=True)
                        completed = active_or_failed = "N"

                        next_action = input(
                            f"\n{INDENT}Choose an option: [r]efresh, [q]uit, [f]Refine,[g]go back,[s]Sucess, [a]running or failed, [c]Counts, [e] copy cells: : "
                        ).strip().lower()

                        if next_action == "r":
                            continue
                        elif next_action == "g":
                            break
                        elif next_action == "a":
                            active_or_failed = 'y'
                            continue
                        elif next_action == "s":
                            completed = 'y'
                            continue
                        elif next_action == "f":
                            # completed = 'y'
                            num_runs = int(input("Enter the number of Runs"))
                            continue
                        elif next_action == "c":
                            cnt = 'y'
                            continue
                        elif next_action == "e":
                            show_dataframe(df, title="My Data Viewer", geometry="900x450", heading_bg="#3C8DBC", row_sep_color="#E0E6ED", show_row_separators=True, wheel_smooth=True)
                            continue
                        else:
                            break

                elif choice == "2":
                    # --- PERSON ID FLOW ---
                    person_id = input(f"{INDENT}Enter person ID to check: ").strip()
                    person_id = person_id + "@cisco.com"

                    time_input = input(f"{INDENT}Enter time filter (e.g., 1d, 2h, 10m): ").strip()
                    date_clause, period_desc = parse_time_input(time_input)

                    while True:
                        description = f"{INDENT}Latest records for: {person_id} (last {period_desc})"
                        query = f"""
                            select b.* from ( SELECT a.*
                             FROM {catalog_db}.stm_mst a
                             WHERE attribute2 = '{person_id}'
                               {job_stream_filter_clause}
                               AND start_time > {date_clause}
                             ORDER BY
                               a.ttn, a.reqid DESC) b
                         """
                        print(f"[INFO] query::{query}")

                        df = query_to_dataframe(cursor, query)

                        print("\n" + description)
                        if completed == "y":
                            df = df[df["CURRENT_PHASE"] == "Success"]
                        elif active_or_failed == "y":
                            df = df[df["CURRENT_PHASE"] != "Success"]

                        if cnt == 'y':
                            df = df[df['WORKFLOW_TYPE'] != 'SRC2STG'][['TTN', 'TGT_ROWS']]
                        df.insert(0, 'SNO', range(1, len(df) + 1))
                        insert_table_black(df)
                        completed = active_or_failed = "N"
                        next_action = input(
                            f"\n{INDENT}Choose an option: [r]efresh, [f]efine filter, [q]uit, [g]go back [s]Sucess, [a]running or failed,[c]Count,[e]Copy cells  : "
                        ).strip().lower()

                        if next_action == "r":
                            continue  # rerun with same filter
                        elif next_action == "f":
                            time_input = input(f"{INDENT}Enter new time filter (e.g., 1d, 2h, 10m): ").strip()
                            date_clause, period_desc = parse_time_input(time_input)
                            continue
                        elif next_action == "a":
                            active_or_failed = 'y'
                            continue
                        elif next_action == "s":
                            completed = 'y'
                            continue
                        elif next_action == "c":
                            cnt = 'y'
                            continue
                        elif next_action == "e":
                            show_dataframe(df, title="My Data Viewer", geometry="900x450", heading_bg="#3C8DBC", row_sep_color="#E0E6ED", show_row_separators=True, wheel_smooth=True)
                            continue
                        else:
                            break


   except Exception as e:
        print(f"{INDENT}Error: {str(e)}")
        traceback.print_exc()
   finally:
       cursor.close()
def metadata_refresh(conn):
    print(f"{INDENT}1)PRD")
    print(f"{INDENT}2)TS1")
    print(f"{INDENT}3)TS3")
    ans = input("Enter Environment Choice").strip().lower()

    if ans == "1":
        db_key = "EJCRO"
        schema='EJCRO'
        env='PRD'
    elif ans == "2":
        db_key = "EJCTS1"
        schema = 'EJCTS1'
        env = 'TS1'
    elif ans == "3":
        db_key = "EJCTS3"
        schema = 'EJCTS3'
    else:
        print("Invalid Choice, Returning Back")
        return
    print("Select Ingestion Type")
    print(f"{INDENT}1)FI")
    print(f"{INDENT}2)RI")
    ing_type = input("Enter The Choice").lower().strip()
    if ing_type == "1":
        job_stream_id_filter = " AND JOB_STREAM_ID LIKE '%BQ' "
    elif ing_type == "2":
        job_stream_id_filter = " AND JOB_STREAM_ID LIKE '%TD' "
    else:
        print("Invalid Choice, Returning Back")
        return

    tname = input("Enter The table name").strip().upper()
    try:

            with conn.cursor() as cursor:
              while True:

                qry = f"""
                        SELECT * 
                        FROM {schema}.bq_job_streams 
                        WHERE workflow_type = 'SRC2STG'
                          AND target_table_name = '{tname}'
                          {job_stream_id_filter}
                    """
                df=query_to_dataframe(cursor,qry)
                insert_table_black(df)
                # Extract values from dataframe
                source_db = df['SOURCE_DB_NAME'].iloc[0]
                source_schema = df['SOURCE_SCHEMA'].iloc[0]
                source_table = df['SOURCE_TABLE_NAME'].iloc[0]
                source_db_conn = df['SOURCE_DB_CONNECTION'].iloc[0]
                if ing_type=="2":
                    # Build the command
                    cmd = (
                        f"./adhoc_metadata_bqtd.py "
                        f"-e {env} "
                        f"-d {source_db} "
                        f"-s {source_schema} "
                        f"-t {source_table} "
                        f"-c NO_CONNECTION"
                    )

                else:
                    cmd = (
                        f"./adh_metadata.py "
                        f"-e {env} "
                        f"-d {source_db} "
                        f"-s {source_schema} "
                        f"-t {source_table} "
                        f"-c {source_db_conn}"
                    )
                copy_friendly_print(cmd)
                if input("would you like to check any other table [y] Yes or [n] No").lower()=="y":
                    tname = input("Enter The table name").strip().upper()
                    continue
                else:
                    break



    except Exception as e:
        print(f"error->{e}")
def display_metadata(conn):
    print("1)PRD")
    print("2)TS1")
    env = input("Enter Your Choice").strip()
    env_name='TS1' if env=="2" else 'PRD'
    schema = "EJCRO" if env == "1" else "EJCTS1"
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
    with  conn.cursor() as cur:
        tname = input("Enter Table name").strip().upper()
        qry=f""" select * from {schema}.bq_job_streams where target_table_name in ('{tname}') AND {job_stream_filter}
            """
        df=pd.read_sql(qry,conn)
        # log_message(f"[DEBUG] after executing query:{qry}")
        if df.empty:
            log_message("no rows returned,Hence going back")
            return
        df=df[df["WORKFLOW_TYPE"]=='SRC2STG'].iloc[0]
        src_db_name=df["SOURCE_DB_NAME"]
        src_schema=df["SOURCE_SCHEMA"]
        src_table_name = df["SOURCE_TABLE_NAME"]

        qry1 = f""" SELECT *
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
            log_message("no rows returned,Hence going back")
            return;
        else:
            df1.insert(0, "SNO", range(1, len(df1) + 1))
            insert_table_black(df1)
        while True:
            print("[only columns] c, [column sequence] s,[Data type] d,[data length] l")
            ans=input().lower()
            if ans=="c":
                df_sub = df1[['SNO', 'COLUMN_NAME']]
            elif ans=="s":
                df_sub = df1[['COLUMN_NAME','COLUMN_SEQUENCE']]
            elif ans=="d":
                df_sub = df1[['COLUMN_NAME','DATA_TYPE']]
            elif ans=="l":
                df_sub = df1[['COLUMN_NAME','DATA_LENGTH']]
            else:
                break
            insert_table_black(df_sub)
            continue
    return df1
def ingestion(conn):
    print("1)PRD")
    print("2)TS1")
    env=input("Enter Your Choice").strip()
    schema="EJCRO" if env=="1" else "EJCTS1"
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
    # --- Table input ---
    input_tables = input(f"Enter table names you want to ingest: ").strip().upper()
    table_names = input_tables.replace(",", "\n").splitlines()
    table_names = [x.strip() for x in table_names if x.strip()]
    table_names = "(" + ",".join(f"'{table}'" for table in table_names) + ")"
    print(f"[INFO] Tables selected: {table_names}")



    with conn.cursor() as cursor:
        query = f"""
             SELECT * FROM {schema}.bq_job_streams 
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
         if env == "1":
            if table_names.count("'") == 2:  # means only one table inside parentheses
                table_names = table_names.replace("')", "',)")  # add comma before closing
            tuple_data = ast.literal_eval(table_names)
            table_list = list(tuple_data)
            print(f"[INFO] Table list: {table_list}")
            proc_bloc = []

            # --- Generate PL/SQL update blocks per table ---
            sno = 0
            for table_name in table_list:
                qry = f"""
                                       SELECT * 
                                       FROM {schema}.bq_job_streams 
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
                    sno = sno + 1
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
                if input(f"Would you like to execute the above PLSQL Block ").lower().strip() == "y":

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
            # this below variable is for divind table names in the where clause to appear in the screen
            # this below variable is for divind table names in the where clause to appear in the screen
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
            UPDATE {schema}.bq_job_streams 
            SET run_status='P' 
            WHERE target_table_name IN {wrapped} AND {job_stream_filter}
              AND active_ind='Y'
                                 """
            copy_friendly_print(qry)
            if input(f"Would you like to execute the above Query ").lower().strip() == "y":
                user_choice = ask_confirmation(
                    f"Are you sure you want to execute above SQL query ?",
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
         if input("Would you like to refresh the BQ_JOB_STREAM_TABLE For the tables [y] Yes [n] No").lower() == "y":
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
         cmd_list = value.splitlines()
         if len(cmd_list) > 1:
             try:
                 chunk_size = int(input(
                     "Would you like commands to run in a batch of 3,4,5 etc if Yes enter the size of the batch").strip())

             except ValueError:
                 print(" Since user did not enter numeric value returning.")
                 return
         f_cmds = []
         if len(cmd_list) != 1:
             for i in range(0, len(cmd_list), chunk_size):
                 cmd = cmd_list[i:i + chunk_size]
                 f_cmds.append("\n".join(cmd))  # appends the combined string
             final = "nohup bash -c '\n" + "\nwait\n".join(f_cmds) + "\n ' &"
             # print(final)
             copy_friendly_print(final)


def get_ejcro_conn(creds):
    log_message("[INFO] inside get_ejcro_conn Method")

    # Pick the right DB block: EJCRO
    ejcro = creds["EJCRO"]   # <-- This is the missing step!

    # Build DSN
    dsn = cx_Oracle.makedsn(
        ejcro["host"],
        ejcro["port"],
        service_name=ejcro["service"]
    )

    # Create connection
    conn = cx_Oracle.connect(
        user=ejcro["user"],
        password=ejcro["password"],
        dsn=dsn
    )

    log_message(f"[INFO] Connected to Oracle: {ejcro['service']}")
    return conn


def short_cuts(creds):
    conn = None
    try:
        creds = load_db_credentials()
        conn = get_ejcro_conn(creds)   # OPEN CONNECTION ONCE

        while True:
            print("1)Ingestion")
            print("2)Metadata")
            print("3)Run status BQ_DIY_MASTER")
            print("4)JCT Entries Check BQ_JOB_STREAMS")
            print("5)Update BQ_JOB_STREAMS")
            print("6)MERGE KEY")
            print("7)Duplicate check")
            print("x)Exit Shortcuts Menu")

            choice = input("Enter the choice: ").lower().strip()

            if choice == "1":
                ingestion(conn)

            elif choice == "2":
                print("1) Metadadata Display")
                print("2) Metadadata refresh")
                ans=input("Enter the choice").lower()
                if ans=="1":
                 display_metadata(conn)
                elif ans=="2":
                 metadata_refresh(conn)
            elif choice == "3":
                 run_status(conn)
            elif choice == "4":
                 show_streams(conn)

            elif choice == "x":
                print("Exiting Shortcuts Menu…")
                break   # <-- Exit loop, connection will close in finally block

            else:
                print("Invalid choice, try again.")

    except Exception as e:
        print(f"error -> {e}")
        traceback.print_exc()

    finally:
        if conn:
            conn.close()                        # <-- CLOSE CONNECTION ONLY ON EXIT
            print("Oracle connection closed.")
