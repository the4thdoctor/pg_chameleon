#!/usr/bin/env bash
here=`dirname $0`
psql -c "CREATE USER usr_test WITH PASSWORD 'test';" -U postgres
psql -c 'CREATE DATABASE db_test WITH OWNER usr_test;' -U postgres


if [ "${MYVER}" != "5.6" ]
then
	${here}/install_mysql.sh
fi
sudo cp -f ${here}/my${MYVER}.cnf /etc/mysql/conf.d/my.cnf
sudo service mysql restart
sudo cat /var/log/mysql/error.log

wget http://downloads.mysql.com/docs/sakila-db.tar.gz
tar xfz sakila-db.tar.gz

sudo mysql -u root < sakila-db/sakila-schema.sql
sudo mysql -u root < sakila-db/sakila-data.sql

if [ "${MYVER}" != "5.6" ]
then
	sudo mysql -u root < ${here}/setup_mysql.sql
else
	mysql -u root < ${here}/setup_mysql.sql
fi


