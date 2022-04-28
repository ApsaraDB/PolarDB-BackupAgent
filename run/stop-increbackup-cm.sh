#!/bin/bash -x

# User should config the following params
cmgateway=""
backupmachine=""
instanceid=""
hostlist=("" "" "")
port=""
username=""
password=""
passwordbase64=""
database=""

# example
# cmgateway="xx.xx.xx.xx:5500"
# backupmachine="xx.xx.xx.xx:1888"
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

# execute stop backup
curl "http://$cmgateway/manager/StopBackup" -d '{
  "GatewayAddressList": [""],
  "BackupMachineList": ["http://'$backupmachine'"],
  "InstanceID": "'$instanceid'",
  "BackupID": "increbk",
  "BackupType": "Incremental",
  "BackupJobID": "default",
  "BackupAccount": {"User": "'$username'","Password": "'$passwordbase64'","Endpoint": "'$masterEndpoint'","Port": "'$port'"}
}'