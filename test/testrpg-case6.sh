#!/bin/bash

# case 6: 
# action: do full backup -> create extension -> do block backup -> recovery through full backup and block backup
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
echo 'drop extension pageinspect;' >> $PGDATA/test.sql
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
echo "create extension pageinspect;" >> $PGDATA/test.sql
cat $PGDATA/test.sql
psql -d polardb -U polardb -f $PGDATA/test.sql > $PGDATA/genlogfile
cat $PGDATA/genlogfile
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
  "BackupID": "FullBackup-'$date2'",
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