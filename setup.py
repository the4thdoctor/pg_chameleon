from setuptools import setup

setup(name='pg_chameleon',
	version='1.0.alpha.1',
	description='MySql to PostgreSQL replication system',
	url='https://github.com/the4thdoctor/pg_chameleon',
	author='Federico Campoli',
	author_email='4thdoctor.gallifrey@gmail.com',
	license='BSD License',
	packages=['pg_chameleon'],
	zip_safe=False,
	install_requires=[
								'PyMySQL', 
								'argparse', 
								'mysql-replication', 
								'psycopg2', 
								'wsgiref', 
								'PyYAML'
	],)
