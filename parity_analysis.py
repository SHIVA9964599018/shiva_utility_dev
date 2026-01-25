import pandas as pd
from google.cloud import bigquery
from utils import *
from chat_ui import *
import os
from datetime import datetime
import config

INDENT = " " * 10

# def log_message(message):
#     """
#     Local logger for this module.
#     Will print only when config.CONSOLE_PRINT is True.
#     """
#     import config as _config
#     if getattr(_config, "CONSOLE_PRINT", False):
#         __builtins__['print'](message)


def parity_non_prod():
   env= input(f"Enter the environment for Parity check \n          1)TS1\n          2)TS3" ).lower()
   if env=="1":
       db_key='EJCTS1'
       env_name='TS1'
   elif env=="2":
       db_key='EJCTS3'
       env_name = 'TS3'
   else:
       print("Invalid choice reurning back")
       return

   ing_type=input("Enter the Ingsetion Type \n          1)FI\n          2)RI").lower()
   if ing_type=="1":
       job_stream_filter=" AND job_stream_id like '%BQ'"
   elif ing_type=="2":
       job_stream_filter = " AND job_stream_id like '%TD'"
   else:
       print("Invalid Choice, Returning back")
       return

   inp_tab_list=input("Please enter table names in comma separated or line separated").strip().upper()
   inp_tab_list=inp_tab_list.replace(",","\n").splitlines()
   tnames="(" + ",".join([f"'{tname}'" for tname in inp_tab_list if tname.strip()]) + ")"
   print(f"Tables names : {tnames}")
   creds=load_db_credentials()
   fil=""
   with get_dynamic_db_connection(db_key,creds) as conn:
       with conn.cursor() as cur:
           qry=f""" select * from bq_job_streams where target_table_name IN {tnames}
              {job_stream_filter} AND ACTIVE_IND='Y' """
           from utils import query_to_dataframe
           df = query_to_dataframe(cur,qry)
           insert_table_black(df)
           print(f"Now making it 2 columns DF")
           df_subset = df[df['WORKFLOW_TYPE']=='SRC2STG'][['SOURCE_DB_CONNECTION', 'SOURCE_SCHEMA', 'SOURCE_TABLE_NAME', "INCREMENTAL_COLUMN_NAME","SOURCE_DELETED_FLAG"]]
           insert_table_black(df_subset)
           src_qry={}
           for index,row in df_subset.iterrows():
               conn_name=row["SOURCE_DB_CONNECTION"]
               incr_col=row["INCREMENTAL_COLUMN_NAME"]
               flag=row["SOURCE_DELETED_FLAG"]
               # if flag=="A":
               #     fil=" WHERE edwbq_source_deleted_flag='N'"

               fqn="SELECT " + "'" + row["SOURCE_TABLE_NAME"] +"'"+ " as TABLE_NAME,count(1) AS SRC_COUNT FROM " + row["SOURCE_SCHEMA"] + "." + row["SOURCE_TABLE_NAME"] + fil
               src_qry.setdefault(conn_name, []).append(fqn)
           log_message(f"SRC QRY \n {src_qry}")
           comma_sep_dict={key: ",".join(value_list) for key ,value_list in src_qry.items() }
           log_message(f"comma_sep_dict \n{comma_sep_dict}")
           union_dict = {
               key: " \nUNION ALL\n ".join(value_list)
               for key, value_list in src_qry.items()
           }
           log_message(f"union dicitn\n {union_dict}")
           for key,value in union_dict.items():
               log_message(f"{key}->{value}")
               conn=get_connection(env_name,None,key)
               log_message(f"connection:{conn}")
               cur=conn.cursor()
               df=query_to_dataframe(cur,value)
               df.insert(0,'S.No',range(1,len(df)+1))
               insert_table_black(df)
   stream_df=get_job_streams_df(db_key,tnames,job_stream_filter)
   stream_df.insert(0,'S.No',range(1,len(stream_df)+1))
   insert_table_black(stream_df)
   del_flag_df=stream_df[stream_df["WORKFLOW_TYPE"]=='SRC2STG'][["TARGET_TABLE_NAME","SOURCE_DELETED_FLAG"]]
   print("Getting 2 columsn which helps to identify source_deleted falg  has been enabled ")
   insert_table_black(del_flag_df)
   tgt_qry_df=stream_df[stream_df["WORKFLOW_TYPE"]!='SRC2STG']
   print("only those which has the src2br entry")
   # insert_table_black(tgt_qry_df)

   df_new = tgt_qry_df.assign(
       target_table_name=stream_df["TARGET_TABLE_NAME"],
       fqn=f"SELECT "+ "'"+ tgt_qry_df["TARGET_TABLE_NAME"]+"'" + ",COUNT(1) FROM " + tgt_qry_df["TARGET_DB_NAME"] + "." + tgt_qry_df["TARGET_SCHEMA"] + "." + tgt_qry_df["TARGET_TABLE_NAME"]

   )[["target_table_name","fqn"]]
   # print("new columns addition")
   # insert_table_black(df_new)
   df_new.columns=df_new.columns.str.upper()
   print("changed the case of the columns to upper")
   insert_table_black(df_new)
   print("lets join 2 DF's with source deleted flag coming in")
   joined_df=del_flag_df.merge(df_new,on='TARGET_TABLE_NAME',how='inner')[["TARGET_TABLE_NAME","FQN","SOURCE_DELETED_FLAG"]]
   insert_table_black(joined_df)
   sql_query=" \n UNION ALL\n".join(df_new["FQN"].astype(str))
   copy_friendly_print(sql_query)
   #building logic to find the target count
   # tnames_list = [t.strip().strip("'") for t in tnames.strip("()").split(",")]
   # for table in tnames_list:


def get_merge_keys(creds, env_name, catalog_db, src_db, schema, table):
    """
    Returns list of unique key columns for a given table/environment.

    catalog_db: The catalog connection database (EJCRO, EJCTS1, EJCTS3)
    src_db:     The source db instance name (used in catalog query)
    """
    conn = get_dynamic_db_connection(catalog_db, creds)
    if not conn:
        print(f"[ERROR] Connection failed for {catalog_db}.")
        return None
    try:
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
            'db_schema': schema,
            'table_name': table
        }
        with conn.cursor() as cur:
            cur.execute(unique_key_query, params)
            uk_row = cur.fetchone()
            if not uk_row:
                print(f"[WARN] No unique key found for {src_db}.{schema}.{table}.")
                return None
            unique_key_name = uk_row[0]

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
            'db_schema': schema
        }
        with conn.cursor() as cur:
            cur.execute(unique_key_cols_query, params2)
            col_rows = cur.fetchall()
            if not col_rows:
                print(f"[WARN] No unique key columns found for {unique_key_name}.")
                return None
            unique_columns = [r[0] for r in col_rows]
        return unique_columns
    finally:
        conn.close()


def get_bq_client(env):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    key_file = "BQ_PRD_Key.json" if env == "PROD" else "BQ_STG_Key.json"
    key_path = os.path.join(base_dir, key_file)
    return bigquery.Client.from_service_account_json(key_path)


def normalize_timestamp(ts):
    if ts is None:
        return None
    if hasattr(ts, 'to_pydatetime'):
        ts = ts.to_pydatetime()
    if isinstance(ts, datetime):
        ts = ts.replace(microsecond=0, tzinfo=None)
    return ts


def daily_count_difference_for_table(
        table_name,
        group,
        workflow_type,
        *,
        bq_client=None,   # BigQuery client (used when BQ is source or target)
        src_conn=None,    # DB-API connection for SOURCE (used in FI)
        tgt_conn=None,    # DB-API connection for TARGET (used in RI)
        debug=False       # <= turn on verbose logs
):
    """
    Compare per-day counts (by casting incremental timestamp to DATE) between source and target.

    Connections are provided by the caller and are NOT opened/closed here:
      - FI  : source = Teradata/Oracle (src_conn REQUIRED), target = BigQuery (bq_client REQUIRED)
      - RI  : source = BigQuery (bq_client REQUIRED),          target = Teradata/Oracle (tgt_conn REQUIRED)
    """
    import pandas as pd
    import time, datetime, traceback

    def dlog(msg):
        if debug:
            now = datetime.datetime.now().strftime("%H:%M:%S")
            log_message(f"[{now}] [{workflow_type}] [{table_name}] {msg}")

    dlog("ENTER daily_count_difference_for_table()")
    dlog(f"group rows={len(group)}; columns={list(group.columns)}")

    # Validate group
    try:
        src_rows = group[group['WORKFLOW_TYPE'] == 'SRC2STG']
        tgt_rows = group[group['WORKFLOW_TYPE'] != 'SRC2STG']
    except Exception:
        log_message(f"[ERROR] Unexpected 'group' structure for {table_name}")
        traceback.print_exc()
        return

    if src_rows.empty or tgt_rows.empty:
        log_message(f"[ERROR] Required row missing for table {table_name} (src_rows.empty={src_rows.empty}, tgt_rows.empty={tgt_rows.empty})")
        if debug:
            try:
                log_message(str(group[['WORKFLOW_TYPE']]))
            except Exception:
                log_message(str(group.head()))
        return

    src_row = src_rows.iloc[0]
    tgt_row = tgt_rows.iloc[0]

    incr_col = (src_row.get('INCREMENTAL_COLUMN_NAME') or '').strip()
    dlog(f"INCREMENTAL_COLUMN_NAME='{incr_col}'")
    if not incr_col:
        log_message(f"[SKIP] No incremental column for {table_name}")
        return

    # -------- Build & run queries based on workflow_type --------
    src_per_date, tgt_per_date = {}, {}

    if workflow_type == "FI":
        # SOURCE: Teradata/Oracle (via src_conn)
        if src_conn is None:
            raise ValueError("[FI] src_conn is required (Teradata/Oracle source).")

        schema = src_row['SOURCE_SCHEMA']
        table  = src_row['SOURCE_TABLE_NAME']
        src_query = (
            f'SELECT CAST({incr_col} AS DATE) AS dt, COUNT(*) AS src_count '
            f'FROM "{schema}"."{table}" '
            f'GROUP BY CAST({incr_col} AS DATE)'
        )
        dlog(f"SRC query:\n{src_query}")

        t0 = time.perf_counter()
        cur = src_conn.cursor()
        try:
            dlog("Executing SRC query...")
            cur.execute(src_query)
            src_rows_raw = cur.fetchall()
            dlog(f"SRC fetch done: {len(src_rows_raw)} rows in {time.perf_counter() - t0:.3f}s")
        except Exception:
            log_message(f"[ERROR] Source query failed for {table_name}")
            traceback.print_exc()
            return
        finally:
            try:
                cur.close()
            except Exception:
                pass

        # normalize
        try:
            src_per_date = {str(dt): int(cnt) for dt, cnt in src_rows_raw}
        except Exception:
            log_message(f"[ERROR] Failed to normalize SRC rows for {table_name}")
            traceback.print_exc()
            dlog(f"Sample SRC rows: {src_rows_raw[:3] if src_rows_raw else src_rows_raw}")
            return
        dlog(f"SRC per-date keys={len(src_per_date)} sample={list(src_per_date.items())[:3]}")

        # TARGET: BigQuery (via bq_client)
        if bq_client is None:
            raise ValueError("[FI] bq_client is required (BigQuery target).")

        tgt_table_full = f"{tgt_row['TARGET_DB_NAME']}.{tgt_row['TARGET_SCHEMA']}.{tgt_row['TARGET_TABLE_NAME']}"
        tgt_query = (
            f"SELECT CAST({incr_col} AS DATE) AS dt, COUNT(*) AS tgt_count "
            f"FROM `{tgt_table_full}` "
            f"GROUP BY CAST({incr_col} AS DATE)"
        )
        dlog(f"TGT (BQ) query:\n{tgt_query}")

        t0 = time.perf_counter()
        try:
            dlog("Executing TGT (BQ) query...")
            tgt_rows_iter = bq_client.query(tgt_query).result()
            tgt_rows_raw = [(row['dt'], row['tgt_count']) for row in tgt_rows_iter]
            dlog(f"TGT fetch done: {len(tgt_rows_raw)} rows in {time.perf_counter() - t0:.3f}s")
        except Exception:
            log_message(f"[ERROR] Target (BQ) query failed for {table_name}")
            traceback.print_exc()
            return

        try:
            tgt_per_date = {str(dt): int(cnt) for dt, cnt in tgt_rows_raw}
        except Exception:
            log_message(f"[ERROR] Failed to normalize TGT rows for {table_name}")
            traceback.print_exc()
            dlog(f"Sample TGT rows: {tgt_rows_raw[:3] if tgt_rows_raw else tgt_rows_raw}")
            return
        dlog(f"TGT per-date keys={len(tgt_per_date)} sample={list(tgt_per_date.items())[:3]}")

    elif workflow_type == "RI":
        # SOURCE: BigQuery (via bq_client)
        if bq_client is None:
            raise ValueError("[RI] bq_client is required (BigQuery source).")

        src_table_full = f"{src_row['SOURCE_DB_NAME']}.{src_row['SOURCE_SCHEMA']}.{src_row['SOURCE_TABLE_NAME']}"
        src_query = (
            f"SELECT CAST({incr_col} AS DATE) AS dt, COUNT(*) AS src_count "
            f"FROM `{src_table_full}` "
            f"GROUP BY CAST({incr_col} AS DATE)"
        )
        dlog(f"SRC (BQ) query:\n{src_query}")

        t0 = time.perf_counter()
        try:
            dlog("Executing SRC (BQ) query...")
            src_rows_iter = bq_client.query(src_query).result()
            src_rows_raw = [(row['dt'], row['src_count']) for row in src_rows_iter]
            dlog(f"SRC (BQ) fetch done: {len(src_rows_raw)} rows in {time.perf_counter() - t0:.3f}s")
        except Exception:
            log_message(f"[ERROR] Source (BQ) query failed for {table_name}")
            traceback.print_exc()
            return

        try:
            src_per_date = {str(dt): int(cnt) for dt, cnt in src_rows_raw}
        except Exception:
            log_message(f"[ERROR] Failed to normalize SRC (BQ) rows for {table_name}")
            traceback.print_exc()
            dlog(f"Sample SRC rows: {src_rows_raw[:3] if src_rows_raw else src_rows_raw}")
            return
        dlog(f"SRC per-date keys={len(src_per_date)} sample={list(src_per_date.items())[:3]}")

        # TARGET: Teradata/Oracle (via tgt_conn)
        if tgt_conn is None:
            raise ValueError("[RI] tgt_conn is required (Teradata/Oracle target).")

        tgt_schema = tgt_row['TARGET_SCHEMA']
        tgt_table  = tgt_row['TARGET_TABLE_NAME']
        tgt_query = (
            f'SELECT CAST({incr_col} AS DATE) AS dt, COUNT(*) AS tgt_count '
            f'FROM "{tgt_schema}"."{tgt_table}" '
            f'GROUP BY CAST({incr_col} AS DATE)'
        )
        dlog(f"TGT query:\n{tgt_query}")

        t0 = time.perf_counter()
        cur = tgt_conn.cursor()
        try:
            dlog("Executing TGT query...")
            cur.execute(tgt_query)
            tgt_rows_raw = cur.fetchall()
            dlog(f"TGT fetch done: {len(tgt_rows_raw)} rows in {time.perf_counter() - t0:.3f}s")
        except Exception:
            log_message(f"[ERROR] Target query failed for {table_name}")
            traceback.print_exc()
            return
        finally:
            try:
                cur.close()
            except Exception:
                pass

        try:
            tgt_per_date = {str(dt): int(cnt) for dt, cnt in tgt_rows_raw}
        except Exception:
            log_message(f"[ERROR] Failed to normalize TGT rows for {table_name}")
            traceback.print_exc()
            dlog(f"Sample TGT rows: {tgt_rows_raw[:3] if tgt_rows_raw else tgt_rows_raw}")
            return
        dlog(f"TGT per-date keys={len(tgt_per_date)} sample={list(tgt_per_date.items())[:3]}")

    else:
        log_message(f"[ERROR] Unsupported workflow type: {workflow_type}")
        return

    # -------- Merge & print mismatches --------
    dlog("Merging per-date counts...")
    all_dates = set(src_per_date) | set(tgt_per_date)
    dlog(f"Total unique dates: {len(all_dates)}")

    diff_summary = []
    for dt in sorted(all_dates):
        src_cnt = src_per_date.get(dt, 0)
        tgt_cnt = tgt_per_date.get(dt, 0)
        if src_cnt != tgt_cnt:
            diff_summary.append({
                'Date': dt,
                'Source Count': src_cnt,
                'Target Count': tgt_cnt,
                'Difference': src_cnt - tgt_cnt,
            })

    print(f"\nDates with count mismatch for {table_name} ({workflow_type}):")
    print(f"Number of rows in difference: {len(diff_summary)}")
    if diff_summary:
        if debug:
            dlog(f"First 5 diffs: {diff_summary[:5]}")
        insert_table_black(pd.DataFrame(diff_summary))
    else:
        print("All dates are matching for", table_name)

    dlog("EXIT daily_count_difference_for_table()")


def execute_fi_queries(df_job_streams, workflow_type, env, creds):
    if workflow_type != 'FI':
        raise ValueError("This function only supports FI workflow")

    grouped = df_job_streams.groupby('TARGET_TABLE_NAME')
    bq_client = get_bq_client(env)

    # Keep one connection open per src_db and one for metadata lookup
    connections = {}

    # --------------------
    # Counts section
    # --------------------
    queries_by_src_db = {}

    # Metadata connection for SOURCE_DB_TYPE lookup
    if 'META' not in connections:
        connections['META'] = get_dynamic_db_connection('EJCRO', creds)
    meta_conn = connections['META']

    for table_name, group in grouped:
        src_row = group[group['WORKFLOW_TYPE'] == 'SRC2STG'].iloc[0]
        src_db = src_row['SOURCE_DB_NAME']
        schema = src_row['SOURCE_SCHEMA']
        table = src_row['SOURCE_TABLE_NAME']

        # fetch SOURCE_DB_TYPE from bq_diy_master
        cur = meta_conn.cursor()
        cur.execute("""
            SELECT SOURCE_DB_TYPE
            FROM bq_diy_master
            WHERE source_table = :1
              AND ingestion_type = :2
              AND UPPER(NVL(attribute3, '')) LIKE '%BQ'
              AND ROWNUM = 1
        """, (table, 'JCT'))
        row = cur.fetchone()
        cur.close()
        source_db_type = row[0] if row else None
        log_message(f"[INFO] ROWS FETCHED\n {row}")
        log_message(f"[INFO] SOURCE_DB_TYPE \n  {source_db_type}")

        # build query depending on DB type
        if source_db_type == 'ORACLE':
            query = f"SELECT '{table_name}' AS table_name, COUNT(1) AS total FROM \"{schema}\".\"{table}\""
        else:  # assume Teradata or other
            query = f"SELECT CAST('{table_name}' AS VARCHAR(100)) AS table_name, CAST(COUNT(1) AS BIGINT) AS total FROM \"{schema}\".\"{table}\""

        queries_by_src_db.setdefault(src_db, []).append(query)

    source_stats = {}
    for src_db, queries in queries_by_src_db.items():
        if src_db not in connections:
            connections[src_db] = get_dynamic_db_connection(src_db, creds)

        conn = connections[src_db]
        cursor = conn.cursor()
        qry=" UNION ALL ".join(queries)
        log_message(f"Framed Query \n    {qry}")
        cursor.execute(qry)
        rows = cursor.fetchall()
        cursor.close()

        for tbl_name, total in rows:
            source_stats[tbl_name] = {'total': int(total) if total is not None else 0}

    # Get target counts from BigQuery
    results = []
    for table_name, group in grouped:
        tgt_row = group[group['WORKFLOW_TYPE'] != 'SRC2STG'].iloc[0]
        src_row = group[group['WORKFLOW_TYPE'] == 'SRC2STG'].iloc[0]

        table_dict = source_stats.get(table_name, {})
        src_total = table_dict.get('total', 0)

        source_deleted_flag = src_row.get('SOURCE_DELETED_FLAG', '').strip().upper()
        base_query = f"SELECT COUNT(*) AS total FROM `{tgt_row['TARGET_DB_NAME']}.{tgt_row['TARGET_SCHEMA']}.{tgt_row['TARGET_TABLE_NAME']}`"
        if source_deleted_flag == 'A':
            tgt_query = base_query + " WHERE edwbq_source_deleted_flag = 'N'"
        else:
            tgt_query = base_query

        tgt_query_job = bq_client.query(tgt_query)
        tgt_result = tgt_query_job.result()
        tgt_total = list(tgt_result)[0]['total']
        tgt_total = int(tgt_total) if tgt_total is not None else 0

        results.append({
            'TABLE_NAME': table_name,
            'SOURCE_COUNT': src_total,
            'TARGET_COUNT': tgt_total,
            'COUNT DIFF': (src_total - tgt_total),
            'COUNT_MATCHING': (src_total == tgt_total)
        })

    results_df = pd.DataFrame(results)
    print("\nCount Results:\n")
    insert_table_black(results_df)

    # --------------------
    # Timestamps section
    # --------------------
    answer = input("Do you want to fetch the latest timestamps? (y/n): ").strip().lower()
    if answer != 'y':
        print("Skipping timestamp fetch.")
        # Close all connections
        for conn in connections.values():
            conn.close()
        return results_df

    print("Fetching latest timestamps from source and target...")

    timestamp_src_queries_by_db = {}
    timestamp_target_queries = []
    timestamp_results = []

    for table_name, group in grouped:
        src_row = group[group['WORKFLOW_TYPE'] == 'SRC2STG'].iloc[0]
        tgt_row = group[group['WORKFLOW_TYPE'] != 'SRC2STG'].iloc[0]
        incr_col = src_row.get('INCREMENTAL_COLUMN_NAME')

        if not incr_col or str(incr_col).strip() == '':
            timestamp_results.append({
                'TABLE_NAME': table_name,
                'SOURCE_LATEST_TIMESTAMP': None,
                'TARGET_LATEST_TIMESTAMP': None,
                'TIMESTAMP_MATCHING': None
            })
            continue

        src_db = src_row['SOURCE_DB_NAME']
        schema = src_row['SOURCE_SCHEMA']
        table = src_row['SOURCE_TABLE_NAME']

        src_ts_query = f"SELECT CAST('{table_name}' AS VARCHAR(100)) AS table_name, MAX({incr_col}) AS latest_ts FROM \"{schema}\".\"{table}\""
        log_message(f"source query:{src_ts_query}")
        timestamp_src_queries_by_db.setdefault(src_db, []).append(src_ts_query)


        tgt_table_full = f"{tgt_row['TARGET_DB_NAME']}.{tgt_row['TARGET_SCHEMA']}.{tgt_row['TARGET_TABLE_NAME']}"
        tgt_ts_query = f"SELECT '{table_name}' AS table_name, MAX({incr_col}) AS latest_ts FROM `{tgt_table_full}`"
        timestamp_target_queries.append(tgt_ts_query)
        log_message(f"target query:{tgt_ts_query}")

    source_ts_stats = {}
    for src_db, queries in timestamp_src_queries_by_db.items():
        conn = connections.get(src_db)
        try:
            cursor = conn.cursor()
            cursor.execute(" UNION ALL ".join(queries))
            rows = cursor.fetchall()
            cursor.close()
        except Exception:
            # In case connection expired (user waited too long), reconnect
            conn = get_dynamic_db_connection(src_db, creds)
            connections[src_db] = conn
            cursor = conn.cursor()
            cursor.execute(" UNION ALL ".join(queries))
            rows = cursor.fetchall()
            cursor.close()

        for tbl_name, latest_ts in rows:
            source_ts_stats[tbl_name] = latest_ts

    combined_tgt_ts_query = " UNION ALL ".join(timestamp_target_queries)
    tgt_query_job = bq_client.query(combined_tgt_ts_query)
    tgt_result = tgt_query_job.result()
    target_ts_stats = {row['table_name']: row['latest_ts'] for row in tgt_result}

    for table_name, group in grouped:
        src_ts_raw = source_ts_stats.get(table_name)
        tgt_ts_raw = target_ts_stats.get(table_name)

        src_ts = normalize_timestamp(src_ts_raw)
        tgt_ts = normalize_timestamp(tgt_ts_raw)

        if src_ts is None and tgt_ts is None:
            match = None
        else:
            match = src_ts == tgt_ts

        if table_name not in [row['TABLE_NAME'] for row in timestamp_results]:
            timestamp_results.append({
                'TABLE_NAME': table_name,
                'SOURCE_LATEST_TIMESTAMP': src_ts,
                'TARGET_LATEST_TIMESTAMP': tgt_ts,
                'TIMESTAMP_MATCHING': match
            })

    ts_df = pd.DataFrame(timestamp_results)
    print("\nTimestamp Results:\n")
    insert_table_black(ts_df)

    # --------------------
    # Daily differences section
    # --------------------
    answer = input("\nWould you like to print the difference values on daily basis for both source and target? (y/n): ").strip().lower()
    if answer == 'y':
        table_names = list(grouped.groups.keys())
        while True:
            print("\nAvailable tables:")
            for idx, tbl in enumerate(table_names, 1):
                print(f"{idx}. {tbl}")

            selected_idx = input("\nEnter the number corresponding to the table you want to process (or type 'exit' to quit): ").strip()
            if selected_idx.lower() == 'exit':
                print("Exiting daily difference display.")
                break

            try:
                selected_idx = int(selected_idx)
                selected_table = table_names[selected_idx - 1]
            except (ValueError, IndexError):
                print("Invalid selection. Please try again or type 'exit' to quit.")
                continue

            group_selected = grouped.get_group(selected_table)

            daily_count_difference_for_table(
                selected_table,
                group_selected,
                "FI",
                bq_client=bq_client,
                src_conn=connections[group_selected.iloc[0]['SOURCE_DB_NAME']]
            )
    else:
        print("Skipped daily difference display.")

    # --------------------
    # Close all DB connections at the very end
    # --------------------
    for conn in connections.values():
        conn.close()

    return ts_df


def execute_ri_queries(df_job_streams, workflow_type, env, creds):
    """
    RI flow with persistent connections:
      - Open BQ client once
      - Open each target DB connection once (per TARGET_DB_NAME)
      - Reuse for counts, timestamps, and row-level diffs
      - Close all in a finally block
    """
    import pandas as pd

    if workflow_type != 'RI':
        raise ValueError("This function only supports RI workflow")

    grouped = df_job_streams.groupby('TARGET_TABLE_NAME')
    table_names = list(grouped.groups.keys())

    # ---- Open persistent clients/connections ----
    bq_client = get_bq_client(env)  # keep open through entire function
    target_conns = {}               # { tgt_db_name: connection }

    def get_target_conn(tgt_db_name: str):
        """Return a persistent connection for the given target DB (create once)."""
        if tgt_db_name not in target_conns or target_conns[tgt_db_name] is None:
            conn = get_dynamic_db_connection(tgt_db_name, creds)
            if conn is None:
                raise ValueError(f"Could not connect to target DB: {tgt_db_name}")
            target_conns[tgt_db_name] = conn
        return target_conns[tgt_db_name]

    try:
        # -------------------- SOURCE (BQ) COUNTS --------------------
        source_count_queries = []
        for table_name, group in grouped:
            src_row = group[group['WORKFLOW_TYPE'] == 'SRC2STG'].iloc[0]
            src_full_table = f"{src_row['SOURCE_DB_NAME']}.{src_row['SOURCE_SCHEMA']}.{src_row['SOURCE_TABLE_NAME']}"
            query = (
                f"SELECT '{table_name}' AS table_name, COUNT(1) AS total "
                f"FROM `{src_full_table}`"
            )
            source_count_queries.append(query)

        combined_source_query = " UNION ALL ".join(source_count_queries)
        source_result = bq_client.query(combined_source_query).result()
        source_stats = {row['table_name']: int(row['total']) for row in source_result}

        # -------------------- TARGET (TD/Oracle) COUNTS --------------------
        queries_by_tgt_db = {}
        for table_name, group in grouped:
            tgt_row = group[group['WORKFLOW_TYPE'] != 'SRC2STG'].iloc[0]
            tgt_db = tgt_row['TARGET_DB_NAME']
            tgt_schema = tgt_row['TARGET_SCHEMA']
            tgt_table = tgt_row['TARGET_TABLE_NAME']
            query = (
                f"SELECT CAST('{table_name}' AS VARCHAR(100)) AS table_name, CAST(COUNT(1) AS BIGINT) AS total "
                f'FROM "{tgt_schema}"."{tgt_table}"'
            )
            queries_by_tgt_db.setdefault(tgt_db, []).append(query)

        target_stats = {}
        for tgt_db, queries in queries_by_tgt_db.items():
            combined_query = " UNION ALL ".join(queries)
            conn = get_target_conn(tgt_db)            # <-- reuse persistent conn
            cursor = conn.cursor()
            try:
                cursor.execute(combined_query)
                rows = cursor.fetchall()
            finally:
                cursor.close()
            for tbl_name, total in rows:
                target_stats[tbl_name] = int(total) if total is not None else 0

        # -------------------- COUNT COMPARISON --------------------
        results = []
        for table_name, group in grouped:
            src_total = source_stats.get(table_name, 0)
            tgt_total = target_stats.get(table_name, 0)
            results.append({
                'TABLE_NAME': table_name,
                'SOURCE_COUNT': src_total,
                'TARGET_COUNT': tgt_total,
                'COUNT DIFF': (src_total - tgt_total),
                'COUNT_MATCHING': (src_total == tgt_total)
            })

        results_df = pd.DataFrame(results)
        print("\nCount Results (RI):\n")
        insert_table_black(results_df)

        # -------------------- TIMESTAMPS (optional) --------------------
        answer = input("Do you want to fetch the latest timestamps? (y/n): ").strip().lower()
        if answer != 'y':
            print("Skipping timestamp fetch.")
            ts_df = pd.DataFrame(columns=[
                'TABLE_NAME', 'SOURCE_LATEST_TIMESTAMP', 'TARGET_LATEST_TIMESTAMP', 'TIMESTAMP_MATCHING'
            ])
            # continue to row-level diffs menu with the same persistent connections
        else:
            print("Fetching latest timestamps from source and target...")

            # SOURCE (BQ) MAX timestamps
            source_ts_queries = []
            for table_name, group in grouped:
                src_row = group[group['WORKFLOW_TYPE'] == 'SRC2STG'].iloc[0]
                incr_col = src_row.get('INCREMENTAL_COLUMN_NAME')
                if not incr_col or str(incr_col).strip() == '':
                    continue
                src_full_table = f"{src_row['SOURCE_DB_NAME']}.{src_row['SOURCE_SCHEMA']}.{src_row['SOURCE_TABLE_NAME']}"
                src_ts_query = (
                    f"SELECT '{table_name}' AS table_name, MAX({incr_col}) AS latest_ts "
                    f"FROM `{src_full_table}`"
                )
                source_ts_queries.append(src_ts_query)

            source_ts_stats = {}
            if source_ts_queries:
                combined_source_ts_query = " UNION ALL ".join(source_ts_queries)
                source_ts_result = bq_client.query(combined_source_ts_query).result()
                source_ts_stats = {row['table_name']: row['latest_ts'] for row in source_ts_result}

            # TARGET (TD/Oracle) MAX timestamps
            tgt_ts_queries_by_db = {}
            for table_name, group in grouped:
                src_row = group[group['WORKFLOW_TYPE'] == 'SRC2STG'].iloc[0]
                tgt_row = group[group['WORKFLOW_TYPE'] != 'SRC2STG'].iloc[0]
                incr_col = src_row.get('INCREMENTAL_COLUMN_NAME')
                if not incr_col or str(incr_col).strip() == '':
                    continue

                tgt_db = tgt_row['TARGET_DB_NAME']
                tgt_schema = tgt_row['TARGET_SCHEMA']
                tgt_table = tgt_row['TARGET_TABLE_NAME']
                tgt_ts_query = (
                    f"SELECT CAST('{table_name}' AS VARCHAR(100)) AS table_name, MAX({incr_col}) AS latest_ts "
                    f'FROM "{tgt_schema}"."{tgt_table}"'
                )
                tgt_ts_queries_by_db.setdefault(tgt_db, []).append(tgt_ts_query)

            target_ts_stats = {}
            for tgt_db, queries in tgt_ts_queries_by_db.items():
                combined_query = " UNION ALL ".join(queries)
                conn = get_target_conn(tgt_db)        # <-- reuse persistent conn
                cursor = conn.cursor()
                try:
                    cursor.execute(combined_query)
                    rows = cursor.fetchall()
                finally:
                    cursor.close()
                for tbl_name, latest_ts in rows:
                    target_ts_stats[tbl_name] = latest_ts

            # Compare timestamps
            timestamp_results = []
            for table_name, group in grouped:
                src_row = group[group['WORKFLOW_TYPE'] == 'SRC2STG'].iloc[0]
                incr_col = src_row.get('INCREMENTAL_COLUMN_NAME')
                if not incr_col or str(incr_col).strip() == '':
                    timestamp_results.append({
                        'TABLE_NAME': table_name,
                        'SOURCE_LATEST_TIMESTAMP': None,
                        'TARGET_LATEST_TIMESTAMP': None,
                        'TIMESTAMP_MATCHING': None
                    })
                    continue

                src_ts_raw = source_ts_stats.get(table_name)
                tgt_ts_raw = target_ts_stats.get(table_name)
                src_ts = normalize_timestamp(src_ts_raw)
                tgt_ts = normalize_timestamp(tgt_ts_raw)
                match = None if (src_ts is None and tgt_ts is None) else (src_ts == tgt_ts)

                timestamp_results.append({
                    'TABLE_NAME': table_name,
                    'SOURCE_LATEST_TIMESTAMP': src_ts,
                    'TARGET_LATEST_TIMESTAMP': tgt_ts,
                    'TIMESTAMP_MATCHING': match
                })

            ts_df = pd.DataFrame(timestamp_results)
            print("\nTimestamp Results (RI):")
            insert_table_black(ts_df)

        # --------- DAILY COUNT DIFF INTERACTIVE MENU ---------
        while True:
            answer = input("\nWould you like to print the daily count differences for a table? (y/n, or type 'exit' to quit): ").strip().lower()
            if answer in ['n', 'exit']:
                print("Exiting daily difference display.")
                break

            print("\nAvailable tables:")
            for idx, tbl in enumerate(table_names, 1):
                print(f"{idx}. {tbl}")

            selected_idx = input("\nEnter the number corresponding to the table you want to process (or type 'exit' to quit): ").strip()
            if selected_idx.lower() == 'exit':
                print("Exiting daily difference display.")
                break

            try:
                selected_idx = int(selected_idx)
                selected_table = table_names[selected_idx - 1]
            except (ValueError, IndexError):
                print("Invalid selection. Please try again or type 'exit' to quit.")
                continue

            group_selected = grouped.get_group(selected_table)
            # If your daily_count_difference_for_table can accept an existing conn/client,
            # pass them in to avoid opening new ones inside that function.
            daily_count_difference_for_table(
                selected_table,
                group_selected,
                workflow_type="RI",
                bq_client=bq_client,
                tgt_conn=get_target_conn(tgt_db)  # <— reuse existing pooled connection
            )

        # --------- Helper for building WHERE clause (unchanged) ---------
        def build_timestamp_to_date_in_clause(col, dates, db_type='bq'):
            if not dates:
                return ""
            if isinstance(dates, str):
                date_str = f"'{dates.strip()}'"
                if db_type == 'bq':
                    return f"WHERE DATE({col}) = {date_str}"
                elif db_type == 'td':
                    return f"WHERE CAST({col} AS DATE) = DATE {date_str}"
                else:
                    raise ValueError("db_type must be 'bq' or 'td'")
            elif isinstance(dates, list):
                if not dates:
                    return ""
                quoted = [f"'{d.strip()}'" for d in dates]
                if db_type == 'bq':
                    return f"WHERE DATE({col}) IN ({','.join(quoted)})"
                elif db_type == 'td':
                    date_literals = [f"DATE '{d.strip()}'" for d in dates]
                    return f"WHERE CAST({col} AS DATE) IN ({','.join(date_literals)})"
                else:
                    raise ValueError("db_type must be 'bq' or 'td'")
            else:
                raise TypeError("dates must be a string or a list of strings.")

        # --- ROW-LEVEL DIFF INTERACTIVE MENU (reuses persistent conns) ---
        while True:
            answer = input(
                "\nDo you want to run row-level diff (TD minus BQ and BQ minus TD) for a table? (y/n, or type 'exit' to quit): "
            ).strip().lower()
            if answer in ['n', 'exit']:
                print("Exiting row-level diff.")
                break

            print("\nAvailable tables:")
            for idx, tbl in enumerate(table_names, 1):
                print(f"{idx}. {tbl}")

            selected_idx = input(
                "\nEnter the number corresponding to the table you want to process (or type 'exit' to quit): "
            ).strip()
            if selected_idx.lower() == 'exit':
                print("Exiting row-level diff.")
                break

            try:
                selected_idx = int(selected_idx)
                selected_table = table_names[selected_idx - 1]
            except (ValueError, IndexError):
                print("Invalid selection. Please try again or type 'exit' to quit.")
                continue

            group_selected = grouped.get_group(selected_table)
            src_row = group_selected[group_selected['WORKFLOW_TYPE'] == 'SRC2STG'].iloc[0]
            tgt_row = group_selected[group_selected['WORKFLOW_TYPE'] != 'SRC2STG'].iloc[0]

            src_db = src_row['SOURCE_DB_NAME']
            src_schema = src_row['SOURCE_SCHEMA']
            src_table = src_row['SOURCE_TABLE_NAME']
            tgt_db = tgt_row['TARGET_DB_NAME']
            tgt_schema = tgt_row['TARGET_SCHEMA']
            tgt_table = tgt_row['TARGET_TABLE_NAME']

            # ENV/CATALOG LOGIC
            if src_db.upper() == "EJCRO":
                env_name = "PRD"
                catalog_db = "EJCRO"
            elif src_db.upper() == "EJCTS1":
                env_name = "TS1"
                catalog_db = "EJCTS1"
            elif src_db.upper() == "EJCTS3":
                env_name = "TS3"
                catalog_db = "EJCTS3"
            else:
                env_name = "PRD"
                catalog_db = "EJCRO"

            merge_keys = get_merge_keys(
                creds, env_name, catalog_db, src_db, src_schema, src_table
            )
            if not merge_keys:
                print("[WARN] No merge keys found in catalog for source table, skipping row-level diff.")
                continue

            incr_col = src_row.get('INCREMENTAL_COLUMN_NAME')
            if not incr_col or str(incr_col).strip() == '':
                print("[WARN] No incremental column found in SRC2STG row. Skipping incremental filter.")
                incr_col = None

            # single start date (optional)
            where_filter_bq = ""
            where_filter_td = ""
            if incr_col:
                date_input = input(
                    f"Enter the start date for {incr_col} to compare records from (YYYY-MM-DD), or leave blank for ALL: "
                ).strip()
                if date_input:
                    where_filter_bq = build_timestamp_to_date_in_clause(incr_col, date_input, db_type='bq')
                    where_filter_td = build_timestamp_to_date_in_clause(incr_col, date_input, db_type='td')

            print(f"\n[INFO] Using source (BQ) merge keys: {merge_keys}")
            if where_filter_bq:
                print(f"[INFO] Filtering by DATES from: {date_input}")

            # --- Fetch only those keys from BQ (source) ---
            bq_key_cols = ', '.join([f"`{col}`" for col in merge_keys])
            src_full_table = f"{src_db}.{src_schema}.{src_table}"
            bq_key_query = f"SELECT {bq_key_cols} FROM `{src_full_table}` {where_filter_bq}"
            print("[DEBUG] BQ Query:\n", bq_key_query)
            bq_key_rows = bq_client.query(bq_key_query).result()
            bq_keys = set(tuple(row[col] for col in merge_keys) for row in bq_key_rows)

            # --- Fetch the same keys from TD/Oracle (target) with persistent conn ---
            tgt_key_cols = ', '.join([f'"{col}"' for col in merge_keys])
            conn = get_target_conn(tgt_db)           # <-- reuse persistent conn
            cursor = conn.cursor()
            try:
                td_key_query = f'SELECT {tgt_key_cols} FROM "{tgt_schema}"."{tgt_table}" {where_filter_td}'
                print("[DEBUG] TD Query:\n", td_key_query)
                cursor.execute(td_key_query)
                td_keys = set(tuple(row) for row in cursor.fetchall())
            finally:
                cursor.close()

            only_in_bq = bq_keys - td_keys
            only_in_td = td_keys - bq_keys

            print(f"\nRecords in BQ but NOT in TD: {len(only_in_bq)}")
            print(f"Records in TD but NOT in BQ: {len(only_in_td)}")

            if only_in_bq:
                print("\nAll records ONLY in BQ (not in TD):")
                df_only_in_bq = pd.DataFrame([dict(zip(merge_keys, key)) for key in only_in_bq])
                insert_table_black(df_only_in_bq)
            else:
                print("\nNo records found ONLY in BQ (not in TD).")

            if only_in_td:
                print("\nAll records ONLY in TD (not in BQ):")
                df_only_in_td = pd.DataFrame([dict(zip(merge_keys, key)) for key in only_in_td])
                insert_table_black(df_only_in_td)
            else:
                print("\nNo records found ONLY in TD (not in BQ).")

        return results_df, ts_df

    finally:
        # ---- Clean shutdown of all connections/clients ----
        for db_name, conn in list(target_conns.items()):
            try:
                if conn is not None:
                    conn.close()
            except Exception as e:
                log_message(f"[WARN] Error closing connection for {db_name}: {e}")

        # BigQuery client close if available
        try:
            close_fn = getattr(bq_client, "close", None)
            if callable(close_fn):
                close_fn()
        except Exception as e:
            log_message(f"[WARN] Error closing BigQuery client: {e}")


def get_job_streams_for_tables(ejcro_conn, table_list, workflow_type):
    log_message(f"[INFO] Opening DB cursor for job stream lookup ...")
    cursor = ejcro_conn.cursor()

    like_pattern = '%BQ' if workflow_type == 'FI' else '%TD'
    log_message(f"[INFO] Workflow type: {workflow_type}, using job_stream_id LIKE pattern: {like_pattern}")

    log_message(f"[INFO] Preparing table list for query: {table_list}")
    # Prepare formatted table names for SQL
    formatted_tables = "','".join(table_list)
    formatted_tables = f"'{formatted_tables}'".replace("','", "','")

    # Query string with named parameters for each table
    table_params = ', '.join([f":tbl{i}" for i in range(len(table_list))])
    log_message(f"INFO:table_params={table_params}")
    query = f"""
    SELECT *
    FROM bq_job_streams
    WHERE --active_ind = 'Y' AND
       job_stream_id LIKE :like_pattern
      AND target_table_name IN ({table_params})
    ORDER BY target_table_name, workflow_type
    """

    log_message(f"[INFO] Final job streams query:\n{query}")
    params = {'like_pattern': like_pattern}
    for i, tbl in enumerate(table_list):
        params[f'tbl{i}'] = tbl
    log_message(f"[INFO] Query parameters: {params}")

    log_message("[INFO] Executing job streams query ...")
    cursor.execute(query, params)

    cols = [desc[0] for desc in cursor.description]
    log_message(f"[INFO] Columns fetched: {cols}")

    rows = cursor.fetchall()
    log_message(f"[INFO] Number of rows fetched: {len(rows)}")

    cursor.close()
    df = pd.DataFrame(rows, columns=cols)
    log_message(f"[INFO] DataFrame created with {len(df)} records.")

    log_message("[INFO] Columns in job streams DataFrame:" + str(df.columns.tolist()))
    return df


def build_count_max_queries(df, workflow_type):
    queries = []

    grouped = df.groupby('TARGET_TABLE_NAME')

    for table_name, group in grouped:
        src2stg_row = group[group['WORKFLOW_TYPE'] == 'SRC2STG']
        if src2stg_row.empty:
            incr_col = 'no_incr_col'
        else:
            incr_col = src2stg_row.iloc[0]['INCREMENTAL_COLUMN_NAME']
            if not incr_col or str(incr_col).strip() == '':
                incr_col = 'no_incr_col'
            else:
                incr_col = str(incr_col).strip()

        src_row = src2stg_row.iloc[0]
        tgt_row = group[group['WORKFLOW_TYPE'] != 'SRC2STG'].iloc[0]

        if workflow_type == 'FI':
            if incr_col != 'no_incr_col':
                src_query = f"SELECT COUNT(*) AS total, MAX({incr_col}) AS latest_ts FROM {src_row['SOURCE_SCHEMA']}.{src_row['SOURCE_TABLE_NAME']}"
            else:
                src_query = f"SELECT COUNT(*) AS total, NULL AS latest_ts FROM {src_row['SOURCE_SCHEMA']}.{src_row['SOURCE_TABLE_NAME']}"

            if incr_col != 'no_incr_col':
                tgt_query = f"SELECT COUNT(*) AS total, MAX({incr_col}) AS latest_ts FROM {tgt_row['TARGET_DB_NAME']}.{tgt_row['TARGET_SCHEMA']}.{tgt_row['TARGET_TABLE_NAME']}"
            else:
                tgt_query = f"SELECT COUNT(*) AS total, NULL AS latest_ts FROM {tgt_row['TARGET_DB_NAME']}.{tgt_row['TARGET_SCHEMA']}.{tgt_row['TARGET_TABLE_NAME']}"

        else:  # RI workflow
            if incr_col != 'no_incr_col':
                src_query = f"SELECT COUNT(*) AS total, MAX({incr_col}) AS latest_ts FROM `{src_row['SOURCE_DB_NAME']}.{src_row['SOURCE_SCHEMA']}.{src_row['SOURCE_TABLE_NAME']}`"
            else:
                src_query = f"SELECT COUNT(*) AS total, NULL AS latest_ts FROM `{src_row['SOURCE_DB_NAME']}.{src_row['SOURCE_SCHEMA']}.{src_row['SOURCE_TABLE_NAME']}`"

            if incr_col != 'no_incr_col':
                tgt_query = f"SELECT COUNT(*) AS total, MAX({incr_col}) AS latest_ts FROM {tgt_row['TARGET_SCHEMA']}.{tgt_row['TARGET_TABLE_NAME']}"
            else:
                tgt_query = f"SELECT COUNT(*) AS total, NULL AS latest_ts FROM {tgt_row['TARGET_SCHEMA']}.{tgt_row['TARGET_TABLE_NAME']}"

        queries.append({
            'target_table_name': table_name,
            'workflow_type': workflow_type,
            'source_query': src_query,
            'target_query': tgt_query
        })

    return queries


def validate_job_streams_ejcro(ejcro_conn, table_list, workflow_type):
    cursor = ejcro_conn.cursor()
    like_pattern = '%BQ' if workflow_type == 'FI' else '%TD'

    for tbl in table_list:
        query = f"""
        SELECT COUNT(*) FROM bq_job_streams
        WHERE active_ind = 'Y'
          AND job_stream_id LIKE :like_pattern
          AND target_table_name = :tbl
        """
        cursor.execute(query, like_pattern=like_pattern, tbl=tbl)
        (count,) = cursor.fetchone()

        if count != 2:
            raise ValueError(f"Table '{tbl}' has {count} active job stream entries, expected exactly 2.")

    cursor.close()
    log_message(f"{INDENT}✅ Validation successful! All tables have exactly 2 active job stream entries.")


def run_parity_analysis():
    print(f"\n{INDENT}📊 Starting Parity Analysis...")

    print(f"{INDENT}Select Environment:")
    print(f"{INDENT}1) PROD")
    print(f"{INDENT}2) NON-PROD")
    env_choice = input(f"{INDENT}Enter choice (1 or 2): ").strip()
    env = {"1": "PROD", "2": "NON-PROD"}.get(env_choice)
    if not env:
        print(f"{INDENT}❌ Invalid environment selection.")
        return
    if env != "PROD":

        parity_non_prod();
        return

    print(f"\n{INDENT}Select Workflow Type:")
    print(f"{INDENT}1) FI (TD/ORACLE → BQ)")
    print(f"{INDENT}2) RI (BQ → TD)")
    wf_choice = input(f"{INDENT}Enter choice (1 or 2): ").strip()
    workflow_type = {"1": "FI", "2": "RI"}.get(wf_choice)
    if not workflow_type:
        print(f"{INDENT}❌ Invalid workflow type.")
        return

    input_tables = input(f"{INDENT}Enter target_table_names (comma or newline separated):\n").strip()
    table_list = [tbl.strip().upper() for tbl in input_tables.replace(",", "\n").splitlines() if tbl.strip()]
    if not table_list:
        print(f"{INDENT}❌ No table names provided.")
        return

    creds = load_db_credentials()
    ejcro_conn = get_dynamic_db_connection("EJCRO", creds)
    if ejcro_conn is None:
        print(f"{INDENT}❌ Could not connect to EJCRO.")
        return

    # ✅ Validate each table separately
    valid_tables = []
    for tbl in table_list:
        try:
            validate_job_streams_ejcro(ejcro_conn, [tbl], workflow_type)
            valid_tables.append(tbl)
        except ValueError as e:
            print(f"{INDENT}❌ Validation error for table {tbl}: {e}")
            continue

    if not valid_tables:
        print(f"{INDENT}❌ No valid tables left after validation. Exiting.")
        return

    # ✅ Process only the valid ones
    df_job_streams = get_job_streams_for_tables(ejcro_conn, valid_tables, workflow_type)
    queries = build_count_max_queries(df_job_streams, workflow_type)

    print(f"\n{INDENT}Count and Max Timestamp Queries for source and target:")

    if workflow_type == 'FI':
        execute_fi_queries(df_job_streams, workflow_type, env, creds)
    elif workflow_type == 'RI':
        execute_ri_queries(df_job_streams, workflow_type, env, creds)
    else:
        raise ValueError("Unsupported workflow type")
