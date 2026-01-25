import teradatasql

# Replace with your actual Teradata credentials and hostname
host = "TDTEST.CISCO.COM"
username = "P_BQ_INGEST"
password = "July@2022#"

# Connection string format
con = teradatasql.connect(
    host=host,
    user=username,
    password=password
)

# Create a cursor and execute a query
cur = con.cursor()
cur.execute("SELECT TOP 5 BK_PRODUCT_ID,BK_CONSTITUENT_PRODUCT_ID FROM COMREFDB_BQ_TS1.N_ITEM_LINK")

columns = [desc[0] for desc in cur.description]
print(columns);
# Print header
print("-" * 80)
print(" | ".join(f"{col:<20}" for col in columns))
print("-" * 80)

# Print rows
for row in cur:
    print(" | ".join(f"{str(val):<20}" for val in row))
print("-" * 80)

# Close connection
cur.close()
con.close()
