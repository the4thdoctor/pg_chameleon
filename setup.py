from setuptools import setup

setup(name='pg_chameleon',
      version='0.1',
      description='Migration script from mysql to postgresql',
      url='https://github.com/the4thdoctor/pg_chameleon',
      author='Federico Campoli',
      author_email='4thdoctor.gallifrey@gmail.com',
      license='GNU General Public License v3 (GPLv3)',
      packages=['pg_chameleon'],
      zip_safe=False,
      install_requires=[
          'SQLAlchemy ==0.7.9',
          'psycopg2 ==2.4.5',
          'MySQL-python== 1.2.3',
      ],)