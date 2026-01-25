# job_status.py

import oracledb as cx_Oracle
import pandas as pd
from utils import load_db_credentials,query_to_dataframe
from chat_ui import *

INDENT = " " * 10


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


def check_run_status():
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
        elif env_choice=="0":
            return
        else:
            print(f"Invalid Choice returning back")
            return

        print(f"{INDENT}Connecting to Oracle {catalog_db}...")


        try:
            creds = load_db_credentials()
            ejcro_cred = creds.get(catalog_db)

            if not ejcro_cred:
                print(f"{INDENT}No credentials found for {catalog_db}.")
                return

            dsn = cx_Oracle.makedsn(
                ejcro_cred["host"],
                ejcro_cred["port"],
                service_name=ejcro_cred["service"]
            )

            conn = cx_Oracle.connect(
                user=ejcro_cred["user"],
                password=ejcro_cred["password"],
                dsn=dsn
            )
            print(f"{INDENT}Connected successfully!")

            # Step 1: Get user input for type (FI/RI)
            while True:
                selected_choice = ""
                active_or_failed='N'
                completed='N'
                cnt='N'

                print(f"{INDENT}Please select the type:")
                print(f"{INDENT}1) FI")
                print(f"{INDENT}2) RI")
                print(f"{INDENT}0) Go Back")
                print(f"{INDENT}9) Main Menu")
                selected_choice = input(f"{INDENT}Enter your choice (1 or 2 or 0): ").strip()
                if selected_choice=="9":
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
                    if choice=="0":
                        break
                    elif choice == "9":
                        return
                    elif choice == "1":
                        # --- TABLE FLOW ---
                        input_tables = input(f"{INDENT}Enter target_table_names (comma or newline separated):\n").strip()
                        table_list = [tbl.strip().upper() for tbl in input_tables.replace(",", "\n").splitlines() if tbl.strip()]
                        if not table_list:
                            raise ValueError("No tables provided.")
                        table_list_sql = ", ".join(f"'{tbl}'" for tbl in table_list)

                        iters_raw = input(
                            f"{INDENT}Show rows AFTER which iteration? Enter a positive integer (e.g., 1, 2, 5, 10): "
                        ).strip()
                        if not iters_raw.isdigit() or int(iters_raw) < 1:
                            raise ValueError("Iterations must be a positive integer (>= 1).")
                        num_runs = int(iters_raw)

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
                                    FROM stm_mst
                                    WHERE ttn IN ({table_list_sql})
                                    {job_stream_filter_clause}
                                      AND workflow_type = 'SRC2STG'
                                  )
                                  WHERE rnk = {num_runs}
                                )
                                SELECT a.*
                                FROM stm_mst a
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

                            cursor = conn.cursor()
                            # cursor.execute(query)
                            # rows = cursor.fetchall()
                            # columns = [col[0] for col in cursor.description]
                            df = query_to_dataframe(cursor, query)
                            cursor.close()

                            # if not rows:
                            #     print(f"{INDENT}No records found for the given input.")
                            # else:


                            print("\n" + description)
                            if completed=="y":
                                df = df[df["CURRENT_PHASE"] == "Success"]
                            elif active_or_failed=="y":
                                df = df[df["CURRENT_PHASE"] != "Success"]
                            df.insert(0, 'SNO', range(1, len(df) + 1))

                            if cnt=='y':
                                df = df[df['WORKFLOW_TYPE'] != 'SRC2STG'][['TTN', 'TGT_ROWS']]
                            insert_table_black(df)
                            # show_dataframe(df, title="My Data Viewer", geometry="900x450", heading_bg="#3C8DBC", row_sep_color="#E0E6ED", show_row_separators=True, wheel_smooth=True)
                            completed = active_or_failed = "N"

                            next_action = input(
                                f"\n{INDENT}Choose an option: [r]efresh, [q]uit, [f]Refine,[g]go back,[s]Sucess, [a]running or failed, [c]Counts: : "
                            ).strip().lower()

                            if next_action == "r":
                                continue
                            elif next_action == "g":
                                break
                            elif  next_action == "a":
                                active_or_failed='y'
                                continue
                            elif  next_action == "s":
                                completed='y'
                                continue
                            elif next_action == "f":
                                #completed = 'y'
                                num_runs=int(input("Enter the number of Runs"))
                                continue
                            elif next_action == "c":
                                cnt='y'
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
                                FROM stm_mst a
                                WHERE attribute2 = '{person_id}'
                                  {job_stream_filter_clause}
                                  AND start_time > {date_clause}
                                ORDER BY
                                  a.ttn, a.reqid DESC) b
                            """

                            cursor = conn.cursor()
                            # cursor.execute(query)
                            # rows = cursor.fetchall()
                            # columns = [col[0] for col in cursor.description]
                            df = query_to_dataframe(cursor, query)
                            cursor.close()

                            # if not rows:
                            #     print(f"{INDENT}No records found for the given input.")
                            # else:


                            print("\n" + description)
                            if completed=="y":
                                df = df[df["CURRENT_PHASE"] == "Success"]
                            elif active_or_failed=="y":
                                df = df[df["CURRENT_PHASE"] != "Success"]

                            if cnt=='y':
                                df = df[df['WORKFLOW_TYPE'] != 'SRC2STG'][['TTN', 'TGT_ROWS']]
                            df.insert(0, 'SNO', range(1, len(df) + 1))
                            insert_table_black(df)
                            # show_dataframe(df, title="My Data Viewer", geometry="900x450", heading_bg="#3C8DBC", row_sep_color="#E0E6ED", show_row_separators=True, wheel_smooth=True)
                            completed=active_or_failed="N"
                            next_action = input(
                                f"\n{INDENT}Choose an option: [r]efresh, [f]efine filter, [q]uit, [g]go back [s]Sucess, [a]running or failed,[c]Count : "
                            ).strip().lower()

                            if next_action == "r":
                                continue  # rerun with same filter
                            elif next_action == "f":
                                time_input = input(f"{INDENT}Enter new time filter (e.g., 1d, 2h, 10m): ").strip()
                                date_clause, period_desc = parse_time_input(time_input)
                                continue
                            elif  next_action == "a":
                                active_or_failed='y'
                                continue
                            elif  next_action == "s":
                                completed='y'
                                continue
                            elif next_action == "c":
                                cnt='y'
                                continue
                            else:
                                break

            # ✅ close connection only once after user exits
                conn.close()

        except Exception as e:
            print(f"{INDENT}Error: {str(e)}")
