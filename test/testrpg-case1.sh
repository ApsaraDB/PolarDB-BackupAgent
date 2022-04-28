#!/bin/bash

# case 1: 
# action: do full backup -> delete some data -> do block backup -> recovery through full backup and block backup
# enviroment: polardbstack

# config some param for test
Instance="testInstance"
srcdev="loop0"
dstdev="loop1"
backupdir="/flash2/backup"

echo '########## stop pg ##########'
pg_ctl stop -D $PGDATA

echo '########## reset conf ##########'
rm -rf $PGDATA/polar_node_static.conf
sed -i "s/$dstdev/$srcdev/g" $PGDATA/postgresql.conf

echo '########## start pg ##########'
pg_ctl start -D $PGDATA
read -p "enter to continue" 

echo '########## prepare origin data ##########'
rm -rf $PGDATA/test.sql
touch $PGDATA/test.sql
echo 'drop table employees;' >> $PGDATA/test.sql
echo 'create table employees (id int primary key not null, name text, age int, addr text, salary int);' >> $PGDATA/test.sql
echo 'insert into employees SELECT generate_series(1,1000) as key,repeat( chr(int4(random()*26)+65),4), (random()*(6^2))::integer,null,(random()*(10^4))::integer;' >> $PGDATA/test.sql
cat $PGDATA/test.sql
psql -d polardb -U polardb -f $PGDATA/test.sql > $PGDATA/genlogfile
sleep 1
rm -rf $PGDATA/test.sql
echo 'excute sql done!'
echo ""
read -p "enter to continue" 

echo '########## start wal backup ##########'
curl "http://127.0.0.1:1888/manager/StartBackup" -d '{
  "GatewayAddressList": [""],
  "BackupMachineList": ["http://127.0.0.1:1888"],
  "InstanceID": "'$Instance'",
  "BackupID": "IncreBackup",
  "BackupType": "Incremental",
  "BackupJobID": "job2021267",
  "StartOffset": 0,
  "CallbackURL": "http://127.0.0.1:1889",
  "BackupPBD": "'$srcdev'",
  "BackupAccount": {"User": "polardb","Password": "cG9sYXJkYg==","Endpoint": "127.0.0.1,198.19.64.3,198.19.64.2","Port": "5432"},
  "Filesystem":"pfs.file",
  "BackupStorageSpace": {"StorageType":"fs", "Locations":{"Local": {"Path": "'$backupdir'"}}}
}'
sleep 2
echo ""
read -p "enter to continue" 

echo '########## start full backup ##########'
date1=$(date "+%Y%m%d-%H%M%S")
curl "http://127.0.0.1:1888/manager/StartBackup" -d '{
  "GatewayAddressList": [""],
  "BackupMachineList": ["http://127.0.0.1:1888"],
  "InstanceID": "'$Instance'",
  "BackupID": "FullBackup-'$date1'",
  "BackupType": "Full",
  "UseBlock": false,
  "BackupJobID": "2021267",
  "StartOffset": 0,
  "CallbackURL": "http://127.0.0.1:1889",
  "BackupPBD": "'$srcdev'",
  "BackupAccount": {"User": "polardb","Password": "cG9sYXJkYg==","Endpoint": "127.0.0.1,198.19.64.3,198.19.64.2","Port": "5432"},
  "Filesystem":"pfs.file",
  "BackupStorageSpace": {"StorageType":"fs", "Locations":{"Local": {"Path": "'$backupdir'"}}}
}'
echo ""
read -p "enter to continue" 

echo '########## make some change ##########'
rm -rf $PGDATA/test.sql
touch $PGDATA/test.sql
echo 'delete from employees where id >= 100 and id <= 900;' >> $PGDATA/test.sql
cat $PGDATA/test.sql
psql -d polardb -U polardb -f $PGDATA/test.sql > $PGDATA/genlogfile
sleep 1
rm -rf $PGDATA/test.sql
echo 'excute sql done!'
echo ""
read -p "enter to continue" 

echo '########## start block backup ##########'
date2=$(date "+%Y%m%d-%H%M%S")
curl "http://127.0.0.1:1888/manager/StartBackup" -d '{
  "GatewayAddressList": [""],
  "BackupMachineList": ["http://127.0.0.1:1888"],
  "InstanceID": "'$Instance'",
  "BackupID": "BlockBackup-'$date2'",
  "BackupType": "Full",
  "UseBlock": true,
  "BackupJobID": "2021267",
  "StartOffset": 0,
  "CallbackURL": "http://127.0.0.1:1889",
  "BackupPBD": "'$srcdev'",
  "BackupAccount": {"User": "polardb","Password": "cG9sYXJkYg==","Endpoint": "127.0.0.1,198.19.64.3,198.19.64.2","Port": "5432"},
  "Filesystem":"pfs.file",
  "BackupStorageSpace": {"StorageType":"fs", "Locations":{"Local": {"Path": "'$backupdir'"}}}
}'
echo ""
read -p "enter to continue" 

echo '########## mkfs pbd ##########'
pfs -C disk mkfs -f $dstdev
echo ""
read -p "enter to continue" 

echo '########## recovery full and block backup set ##########'
curl "http://127.0.0.1:1888/manager/Recovery" -d '{
  "UseBlock": true,
  "GatewayAddressList": ["127.0.0.1:9999"],
  "BackupMachineList": ["http://127.0.0.1:1888"],
  "InstanceID": "'$Instance'",
  "BackupJobID": "2021267",
  "StartOffset": 0,
  "CallbackURL": "http://127.0.0.1:1889",
  "BackupPBD": "'$dstdev'",
  "BackupMetaSource": "db",
  "Full": {"InstanceID": "'$Instance'"},
  "RecoveryTime": 1656503714,
  "BackupAccount": {"User": "polardb","Password": "cG9sYXJkYg==","Endpoint": "127.0.0.1,198.19.64.3,198.19.64.2","Port": "5432"},
  "Filesystem":"pfs.file",
  "DBClusterMetaDir": "",
  "BackupStorageSpace": {"StorageType":"fs", "Locations":{"Local": {"Path": "'$backupdir'"}}}
}'
echo ""
read -p "enter to continue" 

echo '########## stop pg ##########'
pg_ctl stop -D $PGDATA

echo '########## reset conf ##########'
rm -rf $PGDATA/polar_node_static.conf
sed -i "s/$srcdev/$dstdev/g" $PGDATA/postgresql.conf
pfs -C disk read /$dstdev/data/polar_exclusive_backup_label >> $PGDATA/backup_label

echo '########## start pg ##########'
pg_ctl start -D $PGDATA

echo '########## check pg data ##########'
echo "sleep 20 s for recovery"
sleep 20
echo "awake"
rm -rf ~/test.sql
touch ~/test.sql
echo 'select count(*) from employees;' >> ~/test.sql
cat ~/test.sql
psql -h 127.0.0.1 -p 5432 -d polardb -U polardb -f ~/test.sql > ~/genlogfile
sleep 1
rm -rf ~/test.sql
echo 'excute sql done, output should be 199!'
cat ~/genlogfile
rm ~/genlogfile
echo ""
read -p "enter to continue" 