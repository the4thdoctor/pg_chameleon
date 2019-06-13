#!/usr/bin/env bash
sudo service mysql stop
sudo apt-get remove -y mysql-common mysql-server-5.6 mysql-server-core-5.6 mysql-client-5.6 mysql-client-core-5.6
sudo apt-get -y autoremove
sudo apt-get -y autoclean
sudo rm -rf /var/lib/mysql
sudo rm -rf /var/log/mysql
sudo rm -rf /etc/mysql
echo mysql-apt-config mysql-apt-config/enable-repo select mysql-5.7 | sudo debconf-set-selections
wget https://dev.mysql.com/get/mysql-apt-config_0.8.12-1_all.deb
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 0x762E3157
sudo apt-key adv --keyserver keys.gnupg.net --recv-keys 0x5072E1F5 
sudo DEBIAN_FRONTEND=noninteractive dpkg --install mysql-apt-config_0.8.12-1_all.deb



sudo apt-get update -q
sudo DEBIAN_FRONTEND=noninteractive apt-get install -q -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" mysql-server
echo "trying to connect to mysql via socket"
sudo mysql -h localhost -u root -e "SELECT version();"
