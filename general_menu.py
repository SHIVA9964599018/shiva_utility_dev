import oracledb as cx_Oracle

cx_Oracle.init_oracle_client(
    lib_dir=r"C:\Users\shivac\Downloads\instantclient-basic-windows.x64-23.8.0.25.04\instantclient_23_8")
import teradatasql
from google.cloud import bigquery
import pandas as pd
from chat_ui import *
from utils import get_dynamic_db_connection,query_to_dataframe
from parity_analysis import get_job_streams_for_tables
from colorama import init, Fore, Style
init()

INDENT = " " * 10


def metadata_refresh(creds):
    print(f"{INDENT}1)PRD")
    print(f"{INDENT}2)TS1")
    print(f"{INDENT}3)TS3")
    ans = input("Enter Environent Choice").strip().lower()

    if ans == "1":
        db_key = "EJCRO"
    elif ans == "2":
        db_key = "EJCTS1"
    elif ans == "3":
        db_key = "EJCTS3"
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
        with get_dynamic_db_connection(db_key, creds) as conn:
            with conn.cursor() as cursor:
              while True:

                qry = f"""
                        SELECT * 
                        FROM bq_job_streams 
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
                if db_key=="EJCTS1":
                    if ing_type=="2":
                        # Build the command
                        cmd = (
                            f"./DIY_compare_metadata.py  "
                            f"-e TS1 "
                            f"-d {source_db} "
                            f"-s {source_schema} "
                            f"-t {source_table} "
                            f"-c NO_CONNECTION"
                        )
                        cmd1= (
                            f"./adhoc_metadata_bqtd.py "
                            f"-e TS1 "
                            f"-d {source_db} "
                            f"-s {source_schema} "
                            f"-t {source_table} "
                            f"-c NO_CONNECTION"
                        )
                        final_cmd=cmd + "\n" + cmd1
                    else:
                        cmd = (
                            f"./adh_metadata.py "
                            f"-e TS1 "
                            f"-d {source_db} "
                            f"-s {source_schema} "
                            f"-t {source_table} "
                            f"-c {source_db_conn}"
                        )
                    copy_friendly_print(final_cmd)
                    if input("would you like to check any other table [y] Yes or [n] No").lower()=="y":
                        tname = input("Enter The table name").strip().upper()
                        continue
                elif db_key=="EJCRO":
                    if ing_type == "2":
                        # Build the command
                        cmd = (
                            f"./adhoc_metadata_bqtd.py "
                            f"-e PRD "
                            f"-d {source_db} "
                            f"-s {source_schema} "
                            f"-t {source_table} "
                            f"-c NO_CONNECTION"
                        )

                    else:
                        cmd = (
                            f"./adh_metadata.py "
                            f"-e PRD "
                            f"-d {source_db} "
                            f"-s {source_schema} "
                            f"-t {source_table} "
                            f"-c {source_db_conn}"
                        )
                    copy_friendly_print(cmd)
                    if input("would you like to check any other table [y] Yes or [n] No").lower()=="y":
                        tname = input("Enter The table name").strip().upper()
                        continue


    except Exception as e:
        print(f"error->{e}")
def find_job_group_id(creds):
    print(f"{INDENT}1)PRD")
    print(f"{INDENT}2)TS1")
    print(f"{INDENT}3)TS3")
    ans=input("Enter Environent Choice").strip().lower()

    if ans=="1":
        db_key="EJCRO"
    elif ans=="2":
        db_key = "EJCTS1"
    elif ans=="3":
        db_key = "EJCTS3"
    else:
        print("Invalid Choice, Returning Back")
        return

    print("Select Ingestion Type")
    print(f"{INDENT}1)FI")
    print(f"{INDENT}2)RI")
    ing_type = input("Enter The Choice").lower().strip()
    if ing_type == "1":
        job_stream_id_filter=" AND JOB_STREAM_ID LIKE '%BQ' "
    elif ing_type == "2":
        job_stream_id_filter=" AND JOB_STREAM_ID LIKE '%TD' "
    else:
        print("Invalid Choice, Returning Back")
        return

    tname=input("Enter The table name").strip().upper()
    try:
        with get_dynamic_db_connection(db_key, creds) as conn:
            cur = conn.cursor()
            qry = f"""
                SELECT job_group_id 
                FROM bq_job_streams 
                WHERE workflow_type = 'SRC2STG'
                  AND target_table_name = '{tname}'
                  {job_stream_id_filter}
            """
            cur.execute(qry)
            result = cur.fetchone()
            if result and result[0] is not None:
                value = result[0]
                copy_friendly_print(value)
            else:
                copy_friendly_print("⚠️ No job group id found.")

    except Exception as e:
        copy_friendly_print(f"❌ Error fetching job group id returning back: {e}")
        return None


def find_incrmental_column(creds):
    print(f"{INDENT}1)PRD")
    print(f"{INDENT}2)TS1")
    print(f"{INDENT}3)TS3")
    ans=input("Enter Environent Choice").strip().lower()

    if ans=="1":
        db_key="EJCRO"
    elif ans=="2":
        db_key = "EJCTS1"
    elif ans=="3":
        db_key = "EJCTS3"
    else:
        print("Invalid Choice, Returning Back")
        return

    print("Select Ingestion Type")
    print(f"{INDENT}1)FI")
    print(f"{INDENT}2)RI")
    ing_type = input("Enter The Choice").lower().strip()
    if ing_type == "1":
        job_stream_id_filter=" AND JOB_STREAM_ID LIKE '%BQ' "
    elif ing_type == "2":
        job_stream_id_filter=" AND JOB_STREAM_ID LIKE '%TD' "
    else:
        print("Invalid Choice, Returning Back")
        return

    tname=input("Enter The table name").strip().upper()
    try:
        with get_dynamic_db_connection(db_key, creds) as conn:
         while True:
            cur = conn.cursor()
            qry = f"""
                SELECT incremental_column_name 
                FROM bq_job_streams 
                WHERE workflow_type = 'SRC2STG'
                  AND target_table_name = '{tname}'
                  {job_stream_id_filter}
            """
            cur.execute(qry)
            result = cur.fetchone()
            if result and result[0] is not None:
                value = result[0]
                copy_friendly_print(value)
            else:
                copy_friendly_print("⚠️ No incremental_column_name found.")
            if input("Would like to check for any other tabes [y] Yes or [n] No").lower().strip()=='y':
                tname = input("Enter The table name").strip().upper()
                continue
    except Exception as e:
        copy_friendly_print(f"❌ Error fetching incremental_column_name: {e}")
        return None


def print_df_copy_friendly(df):
    # Prints tab-separated for Excel-friendliness
    print("\n" + df.to_csv(sep='\t', index=False))


def get_connection_name(tname, db_name):
    from utils import load_db_credentials, get_dynamic_db_connection

    creds = load_db_credentials()
    conn = get_dynamic_db_connection(db_name, creds)
    try:
        cur = conn.cursor()
        qry = """
            SELECT SOURCE_DB_CONNECTION
            FROM bq_job_streams
            WHERE target_table_name = :tname
              AND workflow_type = 'SRC2STG'
              AND job_stream_id LIKE '%BQ'
        """
        cur.execute(qry, {"tname": tname})
        row = cur.fetchone()   # since expecting 1 row
        if row:
            return row[0]
        else:
            return None
    finally:
        # ensure cleanup even if error happens
        cur.close()
        conn.close()


def find_owner():
    try:
            while True:
                # Read input from user
                input_tables = input("Enter table names (comma or newline separated): ").strip().upper()

                # Replace commas with newlines for uniform splitting
                table_names = input_tables.replace(",", "\n")

                # Split into list, strip spaces, and remove empties
                table_list = [tbl.strip() for tbl in table_names.split("\n") if tbl.strip()]

                # If nothing entered, just exit early
                if not table_list:
                    print("No tables entered. Returning to main menu...")
                    return

                # Wrap each in single quotes and join with commas
                final_str = "(" + ",".join(f"'{tbl}'" for tbl in table_list) + ")"

                client = bigquery.Client.from_service_account_json("BQ_PRD_Key.json")
                query = f"""
                    SELECT split(i.table_id,'~')[OFFSET(4)] AS table_name, i.*
                    FROM `gcp-edwprddata-prd-33200.SSDB.PARITY_PRJ_TABS` i
                    WHERE split(i.table_id,'~')[OFFSET(4)] IN {final_str}
                """

                df = client.query(query).to_dataframe()
                df.insert(0, "SNO", range(1, len(df) + 1))
                if df.empty:
                    print("No rows found")
                else:
                    insert_table_black(df)
                answer= input("Would you like to check any other tables y/n")
                if answer=="y":
                    continue
                else:
                    break

    except Exception as e:
        print(f"⚠️ Error: {e}")
        print("Returning to main menu...")

    finally:
        print("Returning to main menu...")

# All other methods unchanged...
def find_db_creds(creds,env_name1,conn_name1=None):
    env_name=None
    env_map = {
        "TS1": "EJCTS1",
        "TS3": "EJCTS3",
        "PRD": "EJCRO"
    }
    if conn_name1 is  None:
        if env_name1 is None:
            print(f"{INDENT}1)PROD")
            print(f"{INDENT}2)NON-PROD")
            env_choice = input(f"{INDENT}Enter environment : ").strip().upper()
            if env_choice == "1":
                db_name = "EJCRO"
                env_name='PRD'
            elif env_choice == "2":
                print("Select NON-PROD DB:")
                print("1) EJCTS1")
                print("2) EJCTS3")
                np_choice = input("Enter your choice (1/2): ").strip()
                db_name = "EJCTS1" if np_choice == "1" else "EJCTS3"
                env_name = "TS1" if db_name=="EJCTS1" else "TS3"
            else:
                print("[ERROR] Invalid environment choice.")
                return None, None
            conn_name = input("Enter connection name if you do not have it enter [n] No: ").strip().upper()
            if conn_name=='N':
                tname=input("Enter Table Name").strip().upper()
                conn_name=get_connection_name(tname,db_name)


            print(f"[INFO] Connecting to {db_name} ...")
        elif env_name1 is not None:
             db_name=env_map.get(env_name1)
             print(f"[INFO] Connecting to {db_name} ...")
    else:
        conn_name=conn_name1
        db_name = env_map.get(env_name1)
        print(f"[INFO] Connecting to {db_name} ...")

    with get_dynamic_db_connection(db_name, creds) as conn:
        if not conn:
            print(f"[ERROR] Connection failed for {db_name}.")
            return None, None
        print(f"[INFO] Connected to {db_name}.")
        cur = conn.cursor()
        query=f"""select parameter_name,to_char(parameter_value) parameter_value,
                CASE WHEN PARAMETER_NAME = 'SOURCE_LOGIN_PASSWORD'
                THEN UTL_I18N.RAW_TO_CHAR(to_char(a.parameter_value),'AL32UTF8')
                ELSE '' END PASSWORD from EDS_DATA_CATALOG.EDW_PARAMETER a
                where upper(environment_name)=:1 and parameter_type =:2
                """
        if env_name1 is not None:
            result=cur.execute(query,(env_name1,conn_name))
        elif env_name1 is  None:
            result = cur.execute(query, (env_name, conn_name))
        rows=cur.fetchall()
        column = [desc[0] for desc in cur.description]
        df=pd.DataFrame(rows,columns=column)
        print(f"[INFO]conn name passed: {conn_name1}")
        if conn_name1 is  None:
            insert_table_black(df)
        return df



def find_job_status(creds):
    print("Is this for:")
    print("1) PROD")
    print("2) NON-PROD")
    env_choice = input("Enter your choice (1/2): ").strip()

    if env_choice == "1":
        db_name = "EJCRO"
    elif env_choice == "2":
        print("Select NON-PROD DB:")
        print("1) EJCTS1")
        print("2) EJCTS3")
        np_choice = input("Enter your choice (1/2): ").strip()
        db_name = "EJCTS1" if np_choice == "1" else "EJCTS3"
    else:
        print("[ERROR] Invalid environment choice.")
        return None

    # Map DB name to environment string for catalog queries
    if db_name == "EJCRO":
        env_name = "PRD"
    elif db_name == "EJCTS1":
        env_name = "TS1"
    elif db_name == "EJCTS3":
        env_name = "TS3"
    else:
        env_name = "PRD"  # fallback/default

    job_group_id = input(f"Enter JOB_GROUP_ID to check status in {env_name}: ").strip()
    if not job_group_id:
        print("[ERROR] Job group ID cannot be empty.")
        return None
    conn = None
    while True:
        no_of_iteration = input("Enter the number of latest rows: ").strip()
        query = f"""
            SELECT *
            FROM (
                SELECT *
                FROM stm_mst
                WHERE job_group = '{job_group_id}'
                ORDER BY reqid DESC
            )
            WHERE ROWNUM <= {no_of_iteration}
        """

        if conn is None:
            print(f"\nConnecting to {db_name}...")
            conn = get_dynamic_db_connection(db_name, creds)
        if not conn:
            print(f"[ERROR] Could not connect to database: {db_name}")
            return None

        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        print(f"\nLatest {no_of_iteration} job status records:\n")
        insert_table_black(df)
        cursor.close()

        answer = input("Would you like to refine the result? Enter y/n: ").strip()
        if answer.lower() == "y":
            continue
        else:
            break

    # ✅ only close once, after loop ends
    if conn:
        conn.close()

    return df


def find_duplicates(creds):
       while True:
        # Just call find_merge_keys first
        unique_columns, src_row, workflow_type = find_merge_keys(creds)
        if unique_columns is None or src_row is None:
            print("[ERROR] Could not get merge key columns. Exiting duplicate check.")
            return

        src_db = src_row['SOURCE_DB_NAME']
        src_schema = src_row['SOURCE_SCHEMA']
        src_table = src_row['SOURCE_TABLE_NAME']
        src_previous_to_extract_dtm = src_row['PREVIOUS_TO_EXTRACT_DTM']
        incr_col = src_row['INCREMENTAL_COLUMN_NAME']
        if workflow_type == 'FI':
            fq_source_table = f"{src_schema}.{src_table}"
        else:
            fq_source_table = f"{src_db}.{src_schema}.{src_table}"
        group_cols = ", ".join(unique_columns)

        # Initialize where condition as empty
        where_cond = ""

        # Only add WHERE if both values are present
        if src_previous_to_extract_dtm is not None and incr_col is not None:
            # Extract only date part, even if it's a datetime object or string with time
            if hasattr(src_previous_to_extract_dtm, "date"):  # If it's a datetime object
                date_part = str(src_previous_to_extract_dtm.date())
            else:
                date_part = str(src_previous_to_extract_dtm)[:10]  # If it's a string
            if workflow_type == "RI":
                where_cond = f"WHERE DATE({incr_col}) > '{date_part}'"
            elif workflow_type == "FI":
                where_cond = f"WHERE CAST({incr_col} AS DATE) > TO_DATE('{date_part}', 'YYYY-MM-DD')"

        dup_query = f"""
            SELECT {group_cols}, COUNT(*) as dup_count
            FROM {fq_source_table}
            {where_cond}
            GROUP BY {group_cols}
            HAVING COUNT(*) > 1
        """

        print(f"[INFO] Duplicate check SQL for {fq_source_table.upper()}:")
        copy_friendly_print(dup_query.strip())
        ans=input("would you like to check for any other tables [y] yes or [n] no").lower()
        if ans=="y":
               continue
        else:
               break;
    # Optionally execute dup_query and print results here!

def find_merge_keys(creds,called_from=None):
    print("Is this for:")
    print("1) PROD")
    print("2) NON-PROD")
    env_choice = input("Enter your choice (1/2): ").strip()

    if env_choice == "1":
        db_name = "EJCRO"
    elif env_choice == "2":
        print("Select NON-PROD DB:")
        print("1) EJCTS1")
        print("2) EJCTS3")
        np_choice = input("Enter your choice (1/2): ").strip()
        db_name = "EJCTS1" if np_choice == "1" else "EJCTS3"
    else:
        print("[ERROR] Invalid environment choice.")
        return None, None

    # Map DB name to environment string for catalog queries
    if db_name == "EJCRO":
        env_name = "PRD"
    elif db_name == "EJCTS1":
        env_name = "TS1"
    elif db_name == "EJCTS3":
        env_name = "TS3"
    else:
        env_name = "PRD"  # fallback/default


    while True:
        print("Is this an RI or FI table?")
        print("1) RI")
        print("2) FI")
        wf_choice = input("Enter your choice (1/2): ").strip()
        if wf_choice == "1":
            workflow_type = "RI"
            break
        elif wf_choice == "2":
            workflow_type = "FI"
            break
        else:
            print("❌ Invalid input! Please enter 1 for RI or 2 for FI.\n")
    conn=None
    while True:
        print("Enter table name (only one table for merge key lookup):")
        table_name = input().strip().upper()
        table_list = [table_name]

        print(f"[INFO] Connecting to {db_name} ...")
        if conn is None:
            conn = get_dynamic_db_connection(db_name, creds)
        if not conn:
            print(f"[ERROR] Connection failed for {db_name}.")
            return None, None
        print(f"[INFO] Connected to {db_name}.")

        try:
            print("[INFO] Fetching job stream metadata ...")
            job_streams = get_job_streams_for_tables(conn, table_list, workflow_type)
            col_workflow = 'WORKFLOW_TYPE'
            col_db = 'SOURCE_DB_NAME'
            col_schema = 'SOURCE_SCHEMA'
            col_table = 'SOURCE_TABLE_NAME'

            src_df = job_streams[job_streams[col_workflow].str.upper() == 'SRC2STG']
            if src_df.empty:
                print("[WARN] No SRC2STG row found for your table in job streams.")
                return None, None
            row = src_df.iloc[0]
            src_db = row['SOURCE_DB_NAME']
            src_schema = row['SOURCE_SCHEMA']
            src_table = row['SOURCE_TABLE_NAME']
            fq_source_table = f"{src_db}.{src_schema}.{src_table}"
            print(f"\n[INFO] Looking up unique key for {fq_source_table.upper()} ...")

            # Find unique key columns (same logic as before)
            unique_key_query = f"""
                SELECT UNIQUE_KEY_NAME
                FROM EDS_DATA_CATALOG.EDWBQ_UNIQUE_KEY
                WHERE ENVIRONMENT_NAME = :env_name
                  AND UPPER(DB_INSTANCE_NAME) = :db_instance
                  AND DB_SCHEMA_NAME = :db_schema
                  AND TABLE_NAME = :table_name
                  AND ACTIVE_FLAG = 'A'
            """
            params = {
                'env_name': env_name,
                'db_instance': src_db.upper(),
                'db_schema': src_schema,
                'table_name': src_table
            }
            print("[INFO] About to execute Unique Key lookup query:")
            print(unique_key_query)
            print("[INFO] With parameters:", params)

            with conn.cursor() as cur:
                cur.execute(unique_key_query, params)
                uk_row = cur.fetchone()
                if not uk_row:
                    print(f"[WARN] No unique key found for {fq_source_table.upper()}.")
                    return None, None
                unique_key_name = uk_row[0]
                print(f"[INFO] Unique Key Name: {unique_key_name}")

            unique_key_cols_query = f"""
                SELECT COLUMN_NAME
                FROM EDS_DATA_CATALOG.EDWBQ_UNIQUE_KEY_COLUMN
                WHERE UNIQUE_KEY_NAME = :uk_name
                  AND ENVIRONMENT_NAME = :env_name
                  AND UPPER(DB_INSTANCE_NAME) = :db_instance
                  AND UPPER(DB_SCHEMA_NAME) = :db_schema
            """
            params2 = {
                'uk_name': unique_key_name,
                'env_name': env_name,
                'db_instance': src_db.upper(),
                'db_schema': src_schema
            }
            print("[INFO] About to execute Unique Key Columns query:")
            print(unique_key_cols_query)
            print("[INFO] With parameters:", params2)

            with conn.cursor() as cur:
                cur.execute(unique_key_cols_query, params2)
                col_rows = cur.fetchall()
                if not col_rows:
                    print(f"[WARN] No unique key columns found for {unique_key_name}.")
                    return None, None
                unique_columns = [r[0] for r in col_rows]
                comma_separated_column=",".join(unique_columns)
                print(f"[INFO] Unique Key Columns: \n" )
                #copy_friendly_print(unique_columns);
                copy_friendly_print(comma_separated_column)

            if called_from is None:
                return unique_columns, row, workflow_type
            else:
                ans=input("Would like to check merge ket for any other tables [y] Yes or [n] No").lower()
                if ans=="y":
                    continue
                else:
                    return

        except Exception as e:
            print(f"[ERROR] Exception in finding merge keys: {e}")
            return None, None



def query_table():
    print("Table query utility not yet implemented.")



def find_column_names(creds):
    # Fixed list of DB names/keys as per your system
    db_options = [
        ("TDTEST", "teradata"),
        ("TDPROD", "teradata"),
        ("BQ", "bigquery")
    ]
    print("Select DB name:")
    for idx, (db_key, _) in enumerate(db_options, start=1):
        print(f"{idx}) {db_key}")
    db_choice = input(f"Enter your choice (1-{len(db_options)}): ").strip()
    if db_choice not in [str(i) for i in range(1, len(db_options)+1)]:
        print("[ERROR] Invalid DB name choice.")
        return
    db_idx = int(db_choice) - 1
    db_name, db_type = db_options[db_idx]

    table_name = input("Enter table name: ").strip().upper()

    if db_type == "bigquery":
        print("Is this for:")
        print("1) PROD")
        print("2) NON-PROD")
        env_choice = input("Enter your choice (1/2): ").strip()
        if env_choice == "1":
            key_path = "BQ_PRD_Key.json"
        elif env_choice == "2":
            key_path = "BQ_STG_Key.json"
        else:
            print("[ERROR] Invalid environment choice.")
            return
        print(f"[INFO] Connecting to BigQuery using {key_path} ...")
        try:
            from google.cloud import bigquery
            client = bigquery.Client.from_service_account_json(key_path)
            print("[INFO] Connected to BigQuery.")
            project_id = client.project
            # print(f"[INFO] project_id=>{project_id}")
            query = f"""
            SELECT DISTINCT table_schema
            FROM `{project_id}`.`region-us`.INFORMATION_SCHEMA.TABLES
            WHERE table_name ='{table_name}'
            """
            print(f"[INFO] Framed Query=>{query}")
            results = client.query(query).result()
            datasets = [row.table_schema for row in results]
            print("\nAvailable Datasets:")
            for i, ds in enumerate(datasets, 1):
                print(f"{i}) {ds}")
            choice = int(input("Enter the Dataset SNO: "))
            dataset=datasets[choice - 1]
            query = f"""
                select column_name 
                FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.COLUMNS` 
                WHERE LOWER(table_name) = '{table_name.lower()}'
                ORDER BY ordinal_position
            """
            print(f"[INFO] Executing query to get schemas and columns for {project_id}.{dataset}.{table_name} ...")
            print(f"[INFO] Query Formed: {query}")
            df = client.query(query).to_dataframe()
            if df.empty:
                print(f"[INFO] No columns found for table '{table_name}' in dataset '{dataset}'. Please check the name and try again.")
                return
            print(f"[INFO] Available schemas (datasets) and columns for '{table_name}':")
            df.insert(0, "SNO", range(1, len(df)+1))
            insert_table_black(df)

        except Exception as e:
            print(f"[ERROR] Failed to connect/query BigQuery: {e}")
        return

    print(f"[INFO] Connecting to {db_type.upper()} using creds key '{db_name}' ...")
    conn = get_dynamic_db_connection(db_name, creds)
    if not conn:
        print(f"[ERROR] Connection failed for {db_name}.")
        return
    print(f"[INFO] Connected to {db_name}.")

    try:
        if db_type == "oracle":
            query = f"""
                SELECT OWNER, COLUMN_NAME
                FROM ALL_TAB_COLUMNS
                WHERE TABLE_NAME = '{table_name.upper()}'
                ORDER BY OWNER, COLUMN_NAME
            """
            print(f"[INFO] Executing query to get owners and columns for {table_name.upper()} ...")
            df = pd.read_sql(query, conn)
            if df.empty:
                print(f"[INFO] No columns found for table '{table_name}' in Oracle. Please check the name and try again.")
                return
            print(f"[INFO] Available owners (schemas) and columns for '{table_name.upper()}':")
            print("\n" + insert_table_black(df))
            if len(df["OWNER"].unique()) > 1:
                user_input = input("[PROMPT] Do you want columns for a particular owner (schema)? (y/n): ").strip().lower()
                if user_input in ('y', 'yes'):
                    owner = input("Enter owner (schema) name: ").strip().upper()
                    filtered = df[df["OWNER"] == owner]
                    if filtered.empty:
                        print(f"[INFO] No columns found for owner {owner}")
                    else:
                        print(f"[INFO] Columns for {owner}.{table_name.upper()}:")
                        print("\n" + insert_table_black(filtered))
        elif db_type == "teradata":
            db_qry=f"""select DatabaseName
                        FROM DBC.TablesV
                        WHERE TableName = '{table_name}'
                        ORDER BY DatabaseName
                     """
            print(f"[INFO] Framed Query:\n {db_qry}")
            dname_df=pd.read_sql(db_qry, conn)
            print(dname_df.columns)
            print(f"[INFO] converting to list the the Databases names's")
            #insert_table_black(dname_df)
            names=dname_df["DataBaseName"].tolist()
            print(f"Available Databases for the table {table_name}")
            for i,DatabaseName in enumerate(names,1):
                print(f"{i}){DatabaseName}")
            choice=int(input("Enter the SNO"))
            dname=names[choice - 1]
            query = f"""
                SELECT ColumnName 
                FROM dbc.columnsV 
                WHERE TableName = '{table_name.upper()}'
                AND DatabaseName='{dname}'
                ORDER BY ColumnId
            """
            print(f"[INFO] Executing query to get owners and columns for {table_name.upper()} ...")
            print(f"[INFO] Query=> {query}")
            df = pd.read_sql(query, conn)
            if df.empty:
                print(f"[INFO] No columns found for table '{table_name}' in Teradata. Please check the name and try again.")
                return
            print(f"[INFO] Available owners (schemas) and columns for '{table_name.upper()}':")
            df.insert(0,"SNO",range(1,len(df)+1))
            insert_table_black(df)

        else:
            print(f"[ERROR] Unsupported DB type: {db_type}")
            return

    except Exception as e:
        print(f"[ERROR] Failed to fetch column names: {e}")

# general_menu.py
def general_menu(creds):
    while True:
        print("\n-- General Utilities --")
        print("1) Find DB Credentials")
        print("2) Find Duplicates")
        print("3) Find Merge Keys,Incremental column,job group id")
        print("4) Find Status of Job by Job Group ID")
        print("5) Find Column Names")
        print("6) Find Owner Names")
        print("7) Metadata refresh")
        print("0) Back to Main Menu")
        choice = input("Select an option: ").strip()
        if choice == "1":
            find_db_creds(creds,env_name1=None)
        elif choice == "2":
            find_duplicates(creds)
        elif choice == "3":

            print(f"{INDENT} 1) Merge Keys")
            print(f"{INDENT} 2) Incremental Column Name")
            print(f"{INDENT} 3) Job Group Id")
            sub_choice= input("Please select the option").lower()
            if sub_choice=="1":
             find_merge_keys(creds,called_from='y')
            elif sub_choice=="2":
                find_incrmental_column(creds)
            elif    sub_choice == "3":
                find_job_group_id(creds)
            else:
                print("Invalid choice Returning Back")
        elif choice == "4":
            find_job_status(creds)
        elif choice == "5":
            find_column_names(creds)
        elif choice == "6":
            find_owner()
        elif choice == "7":
            metadata_refresh(creds)
        elif choice == "0":
            break
        else:
            print("Invalid choice, please try again.")