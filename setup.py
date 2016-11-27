from setuptools import setup

setup(name='pg_chameleon',
	version='1.a1',
	description='MySql to PostgreSQL replication system',
	url='https://github.com/the4thdoctor/pg_chameleon',
	author='Federico Campoli',
	author_email='4thdoctor.gallifrey@gmail.com',
	license='BSD License',
	packages=['pg_chameleon'],
	download_url = 'https://github.com/the4thdoctor/pg_chameleon/tarball/1.0_ALPHA1',
	install_requires=[
								'PyMySQL==0.7.6', 
								'argparse==1.2.1', 
								'mysql-replication==0.9', 
								'psycopg2==2.6.2', 
								'PyYAML==3.11',
								'sphinx==1.4.6',
								'sphinx-autobuild==0.6.0'

							],
	scripts=['pg_cham']
	)

