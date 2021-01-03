#!/usr/bin/env python3

# This gist contains a direct connection to a local PostgreSQL database
# called "suppliers" where the username and password parameters are "postgres"

# This code is adapted from the tutorial hosted below:
# http://www.postgresqltutorial.com/postgresql-python/connect/

import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

# A function that takes in a PostgreSQL query and outputs a pandas database 
def create_pandas_table(sql_query, database):
    table = pd.read_sql_query(sql_query, database)
    return table

conn = psycopg2.connect(host="localhost", port = 25432, database="healthdata", user="postgres", password="postgres")

# Create a cursor object
cur = conn.cursor()

sleep_query="""
select ss.end_date, ss.duration_ms::float/1000/60/60 AS sleep_hr
from healthdata.healthdata.sleep_session ss 
where ss.active = TRUE
and ss.end_date > '2020-12-01'
order by ss.end_date asc
"""
sleep_info = create_pandas_table(sleep_query,conn)
print(sleep_info)
sleep_info.plot(y='sleep_hr', x='end_date')
plt.show()
# Close the cursor and connection to so the server can allocate
# bandwidth to other requests
cur.close()
conn.close()