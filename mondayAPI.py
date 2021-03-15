import requests
import json
import pprint
import pandas as pd

apiKey = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjQ0ODEwMTcwLCJ1aWQiOjE0MDM4OTc5LCJpYWQiOiIyMDIwLTA1LTA4VDA5OjEwOjUxLjAwMFoiLCJwZXIiOiJtZTp3cml0ZSJ9._mmf2HKX9yD2kqPam_9usEz0j2EP3SsOLyF8-RyH0E0"
apiUrl = "https://api.monday.com/v2/"
headers = {"Authorization": apiKey}

query = "{ boards (ids:1099248177) { items {id name column_values{id text title type} } } }"
data = {"query": query}

r = requests.post(url=apiUrl, json=data, headers=headers)
dictr = r.json()
board = dictr['data']
items = board.get("boards")[0]
itemsVal = items.get("items")
monday_id, monday_code, monday_name = [], [], []
for val in itemsVal:
    monday_id.append(val.get("id"))
    monday_code.append(val.get("name"))
    monday_name.append(val.get("column_values")[0]["text"])

df = pd.DataFrame([monday_id, monday_code, monday_name]).T
df.rename(columns={0: "mondayapi_id", 1: "mondayapi_code", 2: "mondayapi_name"}, inplace=True)
df["mondayapi_id"] = pd.to_numeric(df["mondayapi_id"])
df["mondayapi_code"] = pd.to_numeric(df["mondayapi_code"])
print(df.dtypes)
