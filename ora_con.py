import oracledb

# Replace these with your actual values
host = "scan-prd-3066.cisco.com"  # e.g., "oracle.example.com"
port = 1841  # your custom port
service = "ODSPROD.CISCO.COM"  # e.g., "ORCL"
username = "EJCRO"
password = "Ejcro_32017"

oracledb.init_oracle_client(lib_dir=r"C:\Users\shivac\Downloads\instantclient-basic-windows.x64-23.8.0.25.04\instantclient_23_8")

# Construct DSN (Data Source Name)
dsn = f"{host}:{port}/{service}"

try:
    # Create the connection
    connection = oracledb.connect(
        user=username,
        password=password,
        dsn=dsn
    )

    print("✅ Connected successfully to Oracle!")

    # Example query
    cursor = connection.cursor()
    cursor.execute("SELECT sysdate FROM dual")
    result = cursor.fetchone()
    print("Current DB Time:", result[0])

    # Close resources
    cursor.close()
    connection.close()

except oracledb.Error as e:
    print("❌ Error connecting to Oracle:", e)
