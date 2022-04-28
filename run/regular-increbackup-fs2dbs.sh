#!/bin/bash

# User should config the following params
src=""
gatewaylist=""
namespace=""
instanceid=""
hostlist=("127.0.0.1")
port=""
username=""
password=""
passwordbase64=""
database="postgres"
enableencryption="false"
encryptionpassword=""
workercount=16
compress="true"

# enter execute dir
cd /usr/local/polardb_o_backup_tool_current/bin

# check the params
if [ "$src" == "" ] || [ "$gatewaylist" == "" ] || [ "$instanceid" == "" ] || [ "$port" == "" ] || [ "$username" == "" ] || [ "$password" == "" ] || [ "$database" == "" ]
then
    echo "[ERROR] some param is null"
    exit
fi

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
cd /usr/local/polardb_o_backup_tool_current/bin
./backup_ctl -instance "$instanceid-incre" -plugin increpipeline -increpipeline '{
    "EnableEncryption": '$enableencryption',
    "EncryptionPassword": "'$encryptionpassword'",
    "Compress": '$compress',
    "RealTime": false,
    "WorkerCount": '$workercount',
    "Action": "backup",
    "HostIP": "127.0.0.1",
    "Name": "increpipeline",
    "Force": false,
    "ManagerAddr": ["127.0.0.1:1888"],
    "InstanceID": "'$instanceid'",
    "BackupID": "'$backupid'",
    "Frontend": "fs",
    "Backend":"dbs",
    "Endpoints": {
        "Frontend": {
            "Name": "Frontend",
            "PluginName": "fs",
            "InitFunc": "ExportMapping",
            "ConfPath": "fs.conf"
        },
        "Backend": {
            "Name": "Backend",
            "PluginName": "dbs",
            "InitFunc": "ExportMapping",
            "ConfPath": "dbs.conf"
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
}'  -fs '{
    "Path": "'$src'"
}' -dbs '{
    "GatewayList": ["'$gatewaylist'"],
    "Namespace": "'$namespace'",
    "InstanceID": "'$instanceid'",
    "BackupID": "'$backupid'",
    "Name": "dbs"
}'
