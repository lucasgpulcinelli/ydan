#!/usr/bin/env python3
import os
import time
import psycopg2
import pyarrow as pa
import pyiceberg.catalog
import pyiceberg.exceptions


user = os.getenv("POSTGRES_USER", "postgres")
password = os.getenv("POSTGRES_PASSWORD", "postgres")
host = os.getenv("POSTGRES_HOST", "localhost")
db = os.getenv("POSTGRES_DB", "postgres")
port = int(os.getenv("POSTGRES_PORT", 5432))

conn = psycopg2.connect(
    user=user,
    password=password,
    host=host,
    dbname=db,
    port=port
)

c = conn.cursor()
c.execute("SELECT table_name FROM information_schema.tables")
print(c.fetchall())
conn.close()

catalog = pyiceberg.catalog.load_catalog("default")

try:
    catalog.create_namespace("default")
except pyiceberg.exceptions.NamespaceAlreadyExistsError:
    pass

df = pa.Table.from_arrays(
    [[1, 2], ["a", "b"]],
    schema=pa.schema([
        ("ID", pa.int64()),
        ("NAME", pa.string())
    ])
)

try:
    table = catalog.create_table("default.testing", schema=df.schema)
except pyiceberg.exceptions.TableAlreadyExistsError:
    table = catalog.load_table("default.testing")

table.append(df)

print(table.scan().to_arrow())

time.sleep(1000)
