#!/usr/bin/env python
# -*- coding: utf-8 -*-
import stat
import sys
from os import geteuid, listdir, mkdir, chmod
from os.path import  expanduser, isfile, join
from setuptools import setup

if geteuid() == 0:
	cham_dir = '/usr/local/etc/pg_chameleon'
else:
	cham_dir = "%s/.pg_chameleon" % expanduser('~')	
	
if geteuid() != 0:	
	try:
		mkdir(cham_dir)
		mkdir('%s/logs' % cham_dir)
		mkdir('%s/pid' % cham_dir)
	except:
		pass
	chmod(cham_dir, stat.S_IRWXU)


sql_up_path = 'sql/upgrade'
conf_dir = "%s/config" % cham_dir
sql_dir = "%s/sql" % cham_dir
sql_up_dir = "%s/%s" % (cham_dir, sql_up_path)


data_files = []
conf_files = (conf_dir, ['config/config-example.yaml'])

sql_src = ['sql/create_schema.sql', 'sql/drop_schema.sql']

sql_upgrade = ["%s/%s" % (sql_up_path, file) for file in listdir(sql_up_path) if isfile(join(sql_up_path, file))]

sql_files = (sql_dir,sql_src)
sql_files = (sql_dir,sql_src)
sql_up_files = (sql_up_dir,sql_upgrade)


data_files.append(conf_files)
data_files.append(sql_files)
data_files.append(sql_up_files)
setup(
	name="pg_chameleon",
	version="v1.0-alpha.4",
	description="MySQL to PostgreSQL replica",
	long_description="""Pg_chameleon is a replication tool from MySQL to PostgreSQL developed in Python 2.7 and Python 3.3+
The system relies on the mysql-replication library to pull the changes from MySQL and covert them into a jsonb object. 
A plpgsql function decodes the jsonb and replays the changes into the PostgreSQL database.

The tool requires an  initial replica setup which pulls the data from MySQL in read only mode. 
This is done by the tool running FLUSH TABLE WITH READ LOCK;  .

The tool can pull the data from a cascading replica when the MySQL slave is configured with log-slave-updates.
""",
	author="Federico Campoli",
	author_email="the4thdoctor.gallifrey@gmail.com",
	url="https://github.com/the4thdoctor/pg_chameleon/",
	platforms=[
		"linux"
	],
	classifiers=[
		"License :: OSI Approved :: BSD License"
	],
	#packages=['pg_chameleon'],
	py_modules=[
		"pg_chameleon.__init__",
		"pg_chameleon.lib.global_lib",
		"pg_chameleon.lib.mysql_lib",
		"pg_chameleon.lib.pg_lib",
		"pg_chameleon.lib.sql_util"
	],
	scripts=[
		"scripts/chameleon.py"
	],
	install_requires=[
					'PyMySQL>=0.7.6', 
					'argparse>=1.2.1', 
					'mysql-replication>=0.11', 
					'psycopg2>=2.6.2', 
					'PyYAML>=3.11', 
					'sphinx>=1.4.6', 
					'sphinx-autobuild>=0.6.0'
	],
	data_files = data_files, 
	include_package_data = True
	
)
