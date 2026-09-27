# Databricks notebook source
# =====================================================
# Define Connection Variables (EDIT THESE)
# =====================================================

postgres_host = "ep-flat-flower-b52fwnhv-pooler.c-7.us-east-2.aws.neon.tech"
postgres_port = "5432"
postgres_database = "neondb"
postgres_user = "neondb_owner"
postgres_password = "npg_VPd5YflD1aMH"

# Secret scope name (will be created if not exists)
secret_scope_name = "banking-scope"

# Secret key name (single secret containing full JSON)
secret_key_name = "postgres-connection-json"
# secret_key_name = "postgres-connection-json-dummy"



# COMMAND ----------

# =====================================================
#  Build JSON Object
# =====================================================

import json

connection_config = {
    "host": postgres_host,
    "port": postgres_port,
    "database": postgres_database,
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver"
}

connection_json = json.dumps(connection_config)

print("Generated JSON Configuration:")
print(connection_json)



# COMMAND ----------

# =====================================================
#  Get Databricks API URL and Token
# =====================================================

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

api_url = ctx.apiUrl().getOrElse(None)
api_token = ctx.apiToken().getOrElse(None)

print(api_url)

# DO NOT print the token in real code
# print(api_token)


import requests
import json



# COMMAND ----------

# =====================================================
#  Configuration
# =====================================================

DATABRICKS_INSTANCE = api_url
DATABRICKS_TOKEN = api_token

scope_name = secret_scope_name
backend_type = "DATABRICKS"




# COMMAND ----------

# =====================================================
#  Create Secret Scope
# =====================================================

url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/scopes/create"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": scope_name
}

response = requests.post(
    url,
    headers=headers,
    data=json.dumps(payload)
)

if response.status_code == 200:
    print(f"Secret scope '{scope_name}' created successfully.")
else:
    print("Failed to create secret scope.")
    print("Status Code:", response.status_code)
    print("Response:", response.text)


# COMMAND ----------

# =====================================================
#  Store PostgreSQL Connection JSON
# =====================================================

scope = secret_scope_name
secret_key = secret_key_name
secret_value = connection_json

url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/put"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": scope,
    "key": secret_key,
    "string_value": secret_value
}

response = requests.post(
    url,
    headers=headers,
    data=json.dumps(payload)
)

if response.status_code == 200:
    print(
        f"Secret '{secret_key}' created successfully "
        f"in scope '{scope}'."
    )
else:
    print("Failed to create secret.")
    print("Status:", response.status_code)
    print("Response:", response.text)




# COMMAND ----------

# =====================================================
#  Verify Secret Retrieval
# =====================================================

try:

    retrieved_json = dbutils.secrets.get(
        scope=secret_scope_name,
        key=secret_key_name
    )

    print("Secret retrieved successfully.")

    parsed = json.loads(retrieved_json)

    # Don't print password
    print("Parsed Configuration:")
    print("Host:", parsed["host"])
    print("Port:", parsed["port"])
    print("Database:", parsed["database"])
    print("User:", parsed["user"])
    print("Driver:", parsed["driver"])
    print("Password: [HIDDEN]")

except Exception as e:

    print("Secret verification failed:")
    print(str(e))



# COMMAND ----------

# =====================================================
#  Gmail API Secret
# =====================================================

import requests
import json

scope = secret_scope_name
secret_key = "gmail_app_password"
secret_value = "gzpsrksiiktmdkig"




# COMMAND ----------

# DBTITLE 1,Cell 9
# =====================================================
#  API Endpoint — Gmail API Secret
# =====================================================

url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/put"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": scope,
    "key": secret_key,
    "string_value": secret_value
}


# COMMAND ----------

# =====================================================
# Send Request
# =====================================================

response = requests.post(
    url,
    headers=headers,
    data=json.dumps(payload)
)


# =====================================================
# Output
# =====================================================

if response.status_code == 200:

    print(
        f"Secret '{secret_key}' created successfully "
        f"in scope '{scope}'."
    )

else:

    print("Failed to create secret.")
    print("Status:", response.status_code)
    print("Response:", response.text)

