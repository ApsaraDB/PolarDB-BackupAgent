#!/bin/bash

# case 12: 
# action: do full backup in cmd mode with default cryption -> insert tuples -> wal backup in cmd mode with default cryption 
# -> recovery through full backup and wals by fs
# enviroment: polardbstack

# config some param for test
Instance="testInstance"
srcdev="loop0"
dstdev="loop1"
backupdir="/flash2/backup"
hostlist=("11.167.225.218" "127.0.0.1" "11.167.225.219")
port="5432"
username="polardb"
password="polardb"
passwordbase64="cG9sYXJkYg=="
database="postgres"

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
cat $PGDATA/test.sql
psql -d polardb -U polardb -f $PGDATA/test.sql > $PGDATA/genlogfile
sleep 1
rm -rf $PGDATA/test.sql
echo 'excute sql done!'
echo ""
read -p "enter to continue" 

echo '########## start full backup in cmd mode ##########'
testdir=$(pwd)
echo 'testdir is '$testdir''
# enter execute dir
cd ../run
workdir=$(pwd)
echo 'workdir is '$workdir''
# find the rw endpoint
masterEndpoint=""
for host in ${hostlist[@]}
do
   result=$(./pgsqltool -host $host -port $port -user $username -pwd $password -db $database -cmd ismaster)
   if [ "$result" == "TRUE" ]
   then
      echo "$host is master"
      masterEndpoint=$host
      break
   fi
done
if [[ "$masterEndpoint" == "" ]]
then
    echo "[ERROR] not found master endpoint, please check the hostlist"
    exit
fi
date=$(date "+%Y%m%d-%H%M%S")
./backup_ctl -config $workdir -instance "$Instance-full" -plugin pgpipeline -fs '{
    "Path": "'$backupdir'",
    "InstanceID": "'$Instance'",
    "BackupID": "fbk-'$date'"
}' -pgpipeline '{
    "EnableEncryption": true,
    "Compress": true,
    "WorkerCount": 16,
    "Action": "backup",
    "HostIP": "127.0.0.1",
    "Name": "pgpipeline",
    "Force": false,
    "ManagerAddr": ["127.0.0.1:1888"],
    "InstanceID": "'$Instance'",
    "BackupID": "fbk-'$date'",
    "Frontend": "pfs",
    "Backend":"fs",
    "Endpoints": {
        "pfs": {
            "Type": "Frontend",
            "PluginName": "pfs",
            "InitFunc": "ExportMapping",
            "ConfPath": "pfs.conf"
        },
        "fs": {
            "Type": "Backend",
            "PluginName": "fs",
            "InitFunc": "ExportMapping",
            "ConfPath": "fs.conf"
        }
    },
    "DBHomePath": "",
    "StatusURL": "/manager/BackupStatus",
    "PgDBConf": {
        "Endpoint": "'$masterEndpoint'",
        "Port": "'$port'",
        "Username": "'$username'",
        "Password": "'$passwordbase64'",
        "Database": "'$database'",
        "ApplicationName": "backup-agent"
    }
}' -pfs '{
    "Pbd": "'$srcdev'",
    "Cluster": "disk",
    "Mode": "file",
    "Flags": "backup",
    "Name": "pfs",
    "DBPath": "data"
}'
echo ""
read -p "enter to continue" 

echo '########## make some change ##########'
rm -rf $PGDATA/test.sql
touch $PGDATA/test.sql
echo 'drop table employees;' >> $PGDATA/test.sql
echo 'create table employees (id int primary key not null, name text, age int, addr text, salary int);' >> $PGDATA/test.sql
echo 'insert into employees SELECT generate_series(1,1001) as key,repeat( chr(int4(random()*26)+65),4), (random()*(6^2))::integer,null,(random()*(10^4))::integer;' >> $PGDATA/test.sql
echo 'select pg_switch_wal();' >> $PGDATA/test.sql
cat $PGDATA/test.sql
psql -d polardb -U polardb -f $PGDATA/test.sql > $PGDATA/genlogfile
cat $PGDATA/genlogfile
sleep 1
rm -rf $PGDATA/test.sql
echo 'excute sql done!'
echo ""
read -p "enter to continue" 

echo '########## start incre backup in cmd mode ##########'
# find the rw endpoint
masterEndpoint=""
for host in ${hostlist[@]}
do
   result=$(./pgsqltool -host $host -port $port -user $username -pwd $password -db $database -cmd ismaster)
   if [ "$result" == "TRUE" ]
   then
      echo "$host is master"
      masterEndpoint=$host
      break
   fi
done
if [[ "$masterEndpoint" == "" ]]
then
    echo "[ERROR] not found master endpoint, please check the hostlist"
    exit
fi
# execute incremental backup
backupid="increbk"
./backup_ctl -config $workdir -plugin increpipeline -instance "$Instance-incre" -increpipeline '{
    "EnableEncryption": true,
    "Compress": true,
    "RealTime": false,
    "WorkerCount": 16,
    "Action": "backup",
    "HostIP": "127.0.0.1",
    "Name": "increpipeline",
    "Force": false,
    "ManagerAddr": ["127.0.0.1:1888"],
    "InstanceID": "'$Instance'",
    "BackupID": "'$backupid'",
    "Endpoints": {
        "Frontend": {
            "Name": "Frontend",
            "PluginName": "pfs",
            "InitFunc": "ExportMapping",
            "ConfPath": "pfs.conf"
        },
        "Backend": {
            "Name": "Backend",
            "PluginName": "fs",
            "InitFunc": "ExportMapping",
            "ConfPath": "fs.conf"
        }
    },
    "DBHomePath": "",
    "StatusURL": "/manager/BackupStatus",
    "PgDBConf": {
        "Endpoint": "'$masterEndpoint'",
        "Port": "'$port'",
        "Username": "'$username'",
        "Password": "'$passwordbase64'",
        "Database": "'$database'",
        "ApplicationName": "backup-agent"
    }
}' -pfs '{
    "Pbd": "'$srcdev'",
    "Cluster": "disk",
    "Mode": "file",
    "Flags": "backup",
    "Name": "pfs",
    "DBPath": "data"
}' -fs '{
    "Path": "'$backupdir'",
    "InstanceID": "'$Instance'",
    "BackupID": "'$backupid'"
}'
echo ""
read -p "enter to continue" 

echo '########## mkfs pbd ##########'
pfs -C disk mkfs -f $dstdev
echo ""
read -p "enter to continue" 

echo '########## recovery full and block backup set and wals ##########'
curl "http://127.0.0.1:1888/manager/Recovery" -d '{
  "UseBlock": true,
  "EnableEncryption": true,
  "GatewayAddressList": ["127.0.0.1:9999"],
  "BackupMachineList": ["http://127.0.0.1:1888"],
  "InstanceID": "'$Instance'",
  "BackupJobID": "2021267",
  "StartOffset": 0,
  "CallbackURL": "http://127.0.0.1:1889",
  "BackupPBD": "'$dstdev'",
  "BackupMetaSource": "fs",
  "Full": {"InstanceID": "'$Instance'"},
  "Incremental": {"InstanceID": "'$Instance'", "BackupID": "'$backupid'"},
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