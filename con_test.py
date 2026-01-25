import json
import teradatasql
import oracledb

# Oracle client init only once
oracledb.init_oracle_client(lib_dir=r"C:\Users\shivac\Downloads\instantclient-basic-windows.x64-23.8.0.25.04\instantclient_23_8")

def connect_teradata(config, name):
    try:
        print(f"\n🔗 Connecting to Teradata: {name}")
        conn = teradatasql.connect(
            host=config["host"],
            user=config["user"],
            password=config["password"]
        )
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_TIMESTAMP")
        result = cursor.fetchone()
        print(f"✅ {name} Timestamp: {result[0]}")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error in {name}: {e}")

def connect_oracle(config, name):
    try:
        print(f"\n🔗 Connecting to Oracle: {name}")
        dsn = f"{config['host']}:{config.get('port', 1521)}/{config['service']}"
        conn = oracledb.connect(
            user=config["user"],
            password=config["password"],
            dsn=dsn
        )
        cursor = conn.cursor()
        cursor.execute("SELECT SYSDATE FROM dual")
        result = cursor.fetchone()
        print(f"✅ {name} Date: {result[0]}")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error in {name}: {e}")

# Load all credentials
with open("db_cred.json", "r") as file:
    creds = json.load(file)

# Connect to each
for db_name, config in creds.items():
    db_type = config["type"].lower()
    if db_type == "teradata":
        connect_teradata(config, db_name)
    elif db_type == "oracle":
        connect_oracle(config, db_name)
    else:
        print(f"⚠️ Unknown DB type for {db_name}: {db_type}")
