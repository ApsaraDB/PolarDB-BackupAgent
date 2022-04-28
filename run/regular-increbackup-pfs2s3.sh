#!/bin/bash

# User should config the following params
src=""
s3gateway=""
accesskey=""
secretkey=""
bucket=""
instanceid=""
hostlist=("xx.xx.xx.xx" "xx.xx.xx.xx" "xx.xx.xx.xx")
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
if [ "$src" == "" ] || [ "$s3gateway" == "" ] || [ "$accesskey" == "" ] || [ "$secretkey" == "" ] || [ "$bucket" == "" ]|| [ "$instanceid" == "" ] || [ "$port" == "" ] || [ "$username" == "" ] || [ "$password" == "" ] || [ "$database" == "" ]
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

# switch wal
./pgsqltool -host $masterEndpoint -port $port -user $username -pwd $password -db $database -cmd switchwal

# execute incremental backup
loopname=$(echo $src | cut -d '/' -f 2)
dbpath=/$(echo $src | cut -d '/' -f 3)
backupid="increbk"
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
    "Frontend": "pfs",
    "Backend":"s3",
    "Endpoints": {
        "Frontend": {
            "Name": "Frontend",
            "PluginName": "pfs",
            "InitFunc": "ExportMapping",
            "ConfPath": "pfs.conf"
        },
        "Backend": {
            "Name": "Backend",
            "PluginName": "s3",
            "InitFunc": "ExportMapping",
            "ConfPath": "s3.conf"
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
    "Pbd": "'$loopname'",
    "Cluster": "disk",
    "Mode": "file",
    "Flags": "backup",
    "Name": "pfs",
    "DBPath": "'$dbpath'"
}' -s3 '{
    "Endpoint": "'$s3gateway'",
    "ID": "'$accesskey'",
    "Key": "'$secretkey'",
    "Bucket": "'$bucket'",
    "InstanceID": "'$instanceid'",
    "BackupID": "'$backupid'",
    "Name": "s3"
}'
