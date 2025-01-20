#!/usr/bin/env python
# -*- coding: utf-8 -*-
import setuptools

def readme():
    with open('README.rst') as f:
        return f.read()

package_data = {'pg_chameleon': ['configuration/config-example.yml','sql/upgrade/*.sql','sql/drop_schema.sql','sql/create_schema.sql', 'LICENSE.txt']}

setuptools.setup(
     name="pg_chameleon",
     version="2.0.21",
     description="MySQL to PostgreSQL replica and migration",
    long_description=readme(),
    author = "Federico Campoli",
    author_email = "thedoctor@pgdba.org",
    maintainer = "Federico Campoli",
    maintainer_email = "thedoctor@pgdba.org",
    url="https://codeberg.org/the4thdoctor/pg_chameleon",
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
        'PyMySQL>=0.10.0',
        'mysql-replication>=0.31',
        'psycopg2-binary>=2.8.3',
        'PyYAML>=3.13',
        'tabulate>=0.8.1',
        'daemonize>=2.4.7',
        'rollbar>=0.13.17',
        'parsy>=2.1',
        'Sphinx>=7.4.7'

    ],
    include_package_data = True,
    package_data=package_data,
    packages=setuptools.find_packages(),
    python_requires='>=3.7',
    keywords='postgresql mysql replica migration database',

)
