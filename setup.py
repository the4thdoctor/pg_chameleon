#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup
from distutils.sysconfig import get_python_lib
from os import listdir
from os.path import isfile, join

def readme():
    with open('README.rst') as f:
        return f.read()

python_lib=get_python_lib()

package_data = ('%s/pg_chameleon' % python_lib, ['LICENSE.txt'])

	

sql_up_path = 'sql/upgrade'
conf_dir = "/%s/pg_chameleon/configuration" % python_lib
conn_dir = "/%s/pg_chameleon/connection" % python_lib
sql_dir = "/%s/pg_chameleon/sql" % python_lib
sql_up_dir = "/%s/pg_chameleon/%s" % (python_lib, sql_up_path)


data_files = []
conf_files = (conf_dir, ['configuration/config-example.yml'])

sql_src = ['sql/create_schema.sql', 'sql/drop_schema.sql']
sql_upgrade = ["%s/%s" % (sql_up_path, file) for file in listdir(sql_up_path) if isfile(join(sql_up_path, file))]

sql_files = (sql_dir,sql_src)
sql_up_files = (sql_up_dir,sql_upgrade)

data_files.append(conf_files)
data_files.append(sql_files)
data_files.append(sql_up_files)



setup(
	name="pg_chameleon",
	version="2.0.6",
	description="MySQL to PostgreSQL replica and migration",
	long_description=readme(),
	author = "Federico Campoli",
	author_email = "the4thdoctor.gallifrey@gmail.com",
	maintainer = "Federico Campoli", 
	maintainer_email = "the4thdoctor.gallifrey@gmail.com",
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
		"scripts/chameleon.py", 
		"scripts/chameleon"
	],
	install_requires=[
		'PyMySQL>=0.7.11', 
		'argparse>=1.2.1', 
		'mysql-replication>=0.15', 
		'psycopg2-binary>=2.7.4', 
		'PyYAML>=3.12', 
		'tabulate>=0.8.1', 
		'daemonize>=2.4.7', 
		'rollbar>=0.13.17'
	],
	data_files = data_files, 
	include_package_data = True, 
	python_requires='>=3.3',
	keywords='postgresql mysql replica migration database',
	
)
