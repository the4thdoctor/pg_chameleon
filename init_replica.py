#!/usr/bin/python
from pg_chameleon import mysql_engine
mysql_eng=mysql_engine()
mysql_eng.pull_table_data(limit=30000)
