#!/bin/bash -x

# User should config the following params
cmgateway=""
polardbdata=""
pgdata=""
dst=""
instanceid=""
hostlist=("" "" "")
port=""
username=""
password=""
passwordbase64=""
database=""

# example
# cmgateway="xx.xx.xx.xx:5500"
# polardbdata="/mnt/disk2"
# pgdata="/var/local/polardb/clusters/mycluster"
# dst="xx.xx.xx.xx:8080"
# instanceid="polar-dma-test"
# hostlist=("xx.xx.xx.xx" "xx.xx.xx.xx" "xx.xx.xx.xx")
# port="1521"
# username="user_rep"
# password="pgsql"
# passwordbase64="cGdzcWwK"
# database="postgres"

##########################################

# enter execute dir
cd /usr/local/polardb_o_backup_tool_current/bin

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

backupmachine="$masterEndpoint:1888"

# generate backupid
date=$(date "+%Y%m%d-%H%M%S")
backupid="fbkid-$date"

# execute backup
curl "http://$cmgateway/manager/StartBackup" -d '{
  "GatewayAddressList": [""],
  "BackupMachineList": ["http://'$backupmachine'"],
  "InstanceID": "'$instanceid'",
  "BackupID": "'$backupid'",
  "BackupType": "Full",
  "BackupJobID": "default",
  "CallbackURL": "http://127.0.0.1:1889",
  "BackupAccount": {"User": "'$username'","Password": "'$passwordbase64'","Endpoint": "'$masterEndpoint'","Port": "'$port'"},
  "Filesystem":"fs",
  "BackupStorageSpace": {"StorageType":"http", "Locations":{"http": {"Endpoint": "'$dst'"}}},
  "BackupFolder": "'$polardbdata'",
  "DBClusterMetaDir":"'$pgdata'"
}'