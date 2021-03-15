import sqlalchemy
import requests
import json
import pyarrow.parquet as pq
import pandas as pd
import numpy as np

from google.cloud import bigquery

def parquetToDf():
    # load the parquet file and converts it to a dataframe
    df = pq.read_table(source="cards.parquet").to_pandas()
    df.reset_index(drop=True, inplace=True)

    return df

def mondayapiToDf():
    """ This functions loads the monday api data and converts it to a dataframe"""

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

    return df

def main():
    username = ""
    password = ""
    host = ""
    port = ""
    database = ""

    # connect to the cloud sql database
    dbUrl = f'postgres+psycopg2://{username}:{password}@localhost:{port}/{database}'
    engine = sqlalchemy.create_engine(dbUrl)

    """CREATE TABLE kevin AS
    (SELECT
        inv.id as invoiceId,
        inv.provider as invoiceProvider,
        inv.service_type,
        inv.diagnosed_date,
        inv.invoice_date,
        inv.encounter_id,
        inv.invoiced_amount,
        inv_l.id as invoice_linesId,
        inv_l.provider as invoice_linesProvider,
        inv_l.name as invoice_linename,
        inv_l.quantity,
        inv_l.price,
        enc.id as encountersId,
        enc.provider as encountersProvider,
        enc.encounter_code,
        enc.started_on,
        enc.member_id,
        par.id as parquetsId,
        par.status,
        par.card_type,
        mon.mondayapi_id,
        mon.mondayapi_name
    FROM invoices inv
    INNER JOIN invoice_lines inv_l ON inv.id = inv_l.invoice_id
    INNER JOIN encounters enc ON inv.encounter_id = enc.id
    INNER JOIN parquets par ON enc.member_id = par.beneficiary_id
    INNER JOIN mondayapi mon on inv.provider = mon.mondayapi_code);"""

    # get the parquet dataframe and load it to a table on the database
    parquetDf = parquetToDf()
    parquetDf.to_sql('parquets', con=engine, if_exists="replace", method="multi", index=False)

    # get the monday api dataframe and load it to a table on the database
    mondayapiDf = mondayapiToDf()
    mondayapiDf.to_sql('mondayapi', con=engine, if_exists="replace", index=False)

    # merge all the tables into one table called kevin
    conn = engine.connect()
    dropExistKev = "drop table if exists kevin"
    conn.execute(dropExistKev)
    createKev = "CREATE TABLE kevin AS (SELECT inv.id as invoiceId, inv.provider as invoiceProvider, inv.service_type, inv.diagnosed_date, inv.invoice_date, inv.encounter_id, inv.invoiced_amount, inv_l.id as invoice_linesId, inv_l.provider as invoice_linesProvider, inv_l.name as invoice_linename, inv_l.quantity, inv_l.price, enc.id as encountersId, enc.provider as encountersProvider, enc.encounter_code, enc.started_on, enc.member_id, par.id as parquetsId, par.status, par.card_type,  mon.mondayapi_id, mon.mondayapi_name FROM invoices inv INNER JOIN invoice_lines inv_l ON inv.id = inv_l.invoice_id INNER JOIN encounters enc ON inv.encounter_id = enc.id INNER JOIN parquets par ON enc.member_id = par.beneficiary_id INNER JOIN mondayapi mon on inv.provider = mon.mondayapi_code);"
    conn.execute(createKev)

    kevindf = pd.read_sql_query("select * from kevin", engine)
    # kevindf.to_csv("gs://kevinkarobia/kevin.csv")

    tableId = ""
    client = bigquery.Client()
    client.delete_table(tableId, not_found_ok=True)

    dataset = client.dataset("kevinkarobia")
    tableRef = dataset.table("kevin_claims")
    table = bigquery.Table(tableRef)
    kevTable = client.create_table(table)

    loadJob = client.load_table_from_dataframe(kevindf, kevTable)

    loadJob.result()

    destinationTable = client.get_table(tableId)  # Make an API request.
    print("Loaded {} rows.".format(destinationTable.num_rows))


if __name__ == "__main__":
    main()
