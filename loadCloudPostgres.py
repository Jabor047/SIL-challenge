import sqlalchemy
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import logging
import time


username = "sil-interviews"
password = "sil-family"
host = "35.246.153.86"
port = "5432"
database = "sil-interviews"

# load the parquet file
df = pq.read_table(source="cards.parquet").to_pandas()
df.reset_index(drop=True, inplace=True)

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
    inv_l.name,
    inv_l.quantity,
    inv_l.price,
    enc.id as encountersId,
    enc.provider as encountersProvider,
    enc.encounter_code,
    enc.started_on,
    enc.member_id,
    par.id as parquetsId,
    par.status,
    par.card_type
FROM invoices inv
INNER JOIN invoice_lines inv_l ON inv.id = inv_l.invoice_id
INNER JOIN encounters enc ON inv.encounter_id = enc.id
INNER JOIN parquets par ON enc.member_id = par.beneficiary_id);"""

start_time = time.time()
df.to_sql('parquets', con=engine, if_exists="replace", method="multi", index=False)
print("--- %s seconds ---" % (time.time() - start_time))


conn = engine.connect()
# dropExistPar = "drop table if exists parquets"
# conn.execute(dropExistPar)

dropExistKev = "drop table if exists kevin"
conn.execute(dropExistKev)
createKev = "CREATE TABLE kevin AS (SELECT inv.id as invoiceId, inv.provider as invoiceProvider, inv.service_type, inv.diagnosed_date, inv.invoice_date, inv.encounter_id, inv.invoiced_amount, inv_l.id as invoice_linesId, inv_l.provider as invoice_linesProvider, inv_l.name, inv_l.quantity, inv_l.price, enc.id as encountersId, enc.provider as encountersProvider, enc.encounter_code, enc.started_on, enc.member_id, par.id as parquetsId, par.status, par.card_type FROM invoices inv INNER JOIN invoice_lines inv_l ON inv.id = inv_l.invoice_id INNER JOIN encounters enc ON inv.encounter_id = enc.id INNER JOIN parquets par ON enc.member_id = par.beneficiary_id);"
conn.execute(createKev)

resultSet = conn.execute("select * from kevin limit 2;")
for r in resultSet:
    print(r)
