import requests #pip install requests
import json
#BEFORE YOU RUN THIS SCRIPT, RUN sql-api-generate-jwt.pt

f = open("snowflake_jwt.txt", "r")
bearer_token = f.read()
print('-' * 150)
print('Bearer Token: \n' + str(bearer_token))

url = 'https://HFSMUTH-EVB26016.snowflakecomputing.com/api/v2/statements' #replace HFSMUTH-EVB26016 with the instance ID.

headers = {
            "userAgent": "myApplicationName/1.0",
            "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
            "Content-Type": "application/json",
            "Authorization": "Bearer {btoken}".format(btoken=bearer_token),
            "Accept": "application/json"
           }

data = {
  "statement": "SELECT TOP 10 * from ORDERS",
  "timeout": 60,
  "database": "SNOWFLAKE_SAMPLE_DATA",
  "schema": "TPCH_SF1",
  "warehouse": "COMPUTE_WH",
  "role": "ACCOUNTADMIN"
}

r = requests.post(url, data=json.dumps(data), headers=headers)
print('-' * 150)
print('Response Code: \n' + str(r.status_code))
print('-' * 150)
rjson = json.loads(r.text)
print('Metadata: ')
print(rjson['resultSetMetaData'])
print('-' * 150)
print('Query results:')
for x in rjson['data']:
    print(x)