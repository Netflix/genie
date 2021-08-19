#!/bin/bash
sudo apt-get install -y mysql-client
mysql --host 127.0.0.1 --port 3306 -uroot -ppassword -e "SET GLOBAL innodb_file_format = barracuda; SET GLOBAL innodb_file_per_table = 'on'; SET GLOBAL innodb_file_per_table = 'on'; SET GLOBAL innodb_large_prefix = 'on';  show variables like '%innodb_file%';"
