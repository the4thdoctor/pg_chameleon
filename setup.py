from setuptools import setup

setup(name='pg_chameleon',
	version='0.1',
	description='Migration and replica extension from mysql to postgresql',
	url='https://github.com/the4thdoctor/pg_chameleon',
	author='Federico Campoli',
	author_email='4thdoctor.gallifrey@gmail.com',
	license='BSD License',
	packages=['pg_chameleon'],
	zip_safe=False,
	install_requires=[
								'PyMySQL==0.7.6', 
								'argparse==1.2.1', 
								'mysql-replication==0.9', 
								'psycopg2==2.6.2', 
								'wsgiref==0.1.2', 
								'PyYAML==3.11'
	],)
