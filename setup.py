#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup


setup(
	name="pg_chameleon",
	version="1.0beta1",
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
		"chameleon.py"
	],
	install_requires=[
					'PyMySQL', 
					'argparse', 
					'mysql-replication', 
					'psycopg2', 
					'PyYAML', 
					'sphinx', 
					'sphinx-autobuild'
	],
	data_files=[('/etc/pg_chameleon', ['config/config-example.yaml'])], 
	
	
)
