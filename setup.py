#!/usr/bin/env python
# -*- coding: utf-8 -*-
from os import listdir
from os.path import  isfile, join
from setuptools import setup
from distutils.sysconfig import get_python_lib

python_lib=get_python_lib()

package_data = ('%s/pg_chameleon' % python_lib, ['LICENSE'])

	

sql_up_path = 'sql/upgrade'
conf_dir = "/%s/pg_chameleon/config" % python_lib
sql_dir = "/%s/pg_chameleon/sql" % python_lib
sql_up_dir = "/%s/pg_chameleon/%s" % (python_lib, sql_up_path)


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
	version="v1.3.1",
	description="MySQL to PostgreSQL replica and migration",
	long_description=""" pg_chameleon is a tool for replicating from MySQL to PostgreSQL compatible with Python 2.7 and Python 3.3+.
The system use the library mysql-replication to pull the row images from MySQL which are transformed into a jsonb object. 
A pl/pgsql function decodes the jsonb and replays the changes into the PostgreSQL database.

The tool requires an  initial replica setup which pulls the data from MySQL in read only mode. 
This is done by the tool running FLUSH TABLE WITH READ LOCK;  .

pg_chameleon can pull the data from a cascading replica when the MySQL slave is configured with log-slave-updates.

""",
	author="Federico Campoli",
	author_email="the4thdoctor.gallifrey@gmail.com",
	url="https://github.com/the4thdoctor/pg_chameleon/",
	license="BSD License",
	platforms=[
		"linux"
	],
	classifiers=[
		"License :: OSI Approved :: BSD License",
		"Environment :: Console",
		"Intended Audience :: Developers",
		"Intended Audience :: Information Technology",
		"Intended Audience :: System Administrators",
		"Natural Language :: English",
		"Operating System :: POSIX :: BSD",
		"Operating System :: POSIX :: Linux",
		"Programming Language :: Python",
		"Programming Language :: Python :: 2.7",
		"Programming Language :: Python :: 3",
		"Programming Language :: Python :: 3.3",
		"Programming Language :: Python :: 3.4",
		"Programming Language :: Python :: 3.5",
		"Programming Language :: Python :: 3.6",
		"Topic :: Database :: Database Engines/Servers",
		"Topic :: Other/Nonlisted Topic"
	],
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
		'psycopg2>=2.7.0', 
		'PyYAML>=3.11', 
		'tabulate>=0.7.7', 
					
	],
	data_files = data_files, 
	include_package_data = True
	
)
