import snowflake.connector


# Establish connection to Snowflake
conn = snowflake.connector.connect(
    user='',
    password='',
    account='',  # e.g., 'abc12345.us-east-1'
    warehouse='COMPUTE_WH',         # Optional: name of the Snowflake warehouse
    database='SNOWFLAKE_SAMPLE_DATA',           # Optional: name of the Snowflake database
    schema='TPCH_SF1',               # Optional: name of the Snowflake schema
    role='ACCOUNTADMIN'                    # Optional: the role you want to use
)

# Create a cursor object
cur = conn.cursor()

# Execute a query
cur.execute("SELECT CURRENT_VERSION()")

# Fetch and print the result
one_row = cur.fetchone()
print(f"Snowflake version: {one_row[0]}")

# Close the cursor and the connection
cur.close()
conn.close()
