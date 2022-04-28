#!/bin/bash -x

# User should config the following params
src=""
dst=""
instanceid=""
hostlist=("xx.xx.xx.xx" "xx.xx.xx.xx" "xx.xx.xx.xx")
port=""
username=""
password=""
passwordbase64=""
database="postgres"
enableencryption="false"
encryptionpassword=""

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

date=$(date "+%Y%m%d-%H%M%S")
./backup_ctl -instance "$instanceid-full" -plugin pgpipeline -fs1 '{
    "Path": "'$src'"
}' -fs2 '{
    "Path": "'$dst'",
    "InstanceID": "'$instanceid'",
    "BackupID": "fbk-'$date'"
}' -pgpipeline '{
    "EnableEncryption": '$enableencryption',
    "EncryptionPassword": "'$encryptionpassword'",
    "Compress": true,
    "WorkerCount": 16,
    "Action": "backup",
    "HostIP": "127.0.0.1",
    "Name": "pgpipeline",
    "Force": false,
    "ManagerAddr": ["127.0.0.1:1888"],
    "Frontend": "fs1",
    "Backend":"fs2",
    "InstanceID":"'$instanceid'",
    "BackupID":"fbk-'$date'",
    "Endpoints": {
        "fs1": {
            "Type": "Frontend",
            "PluginName": "fs1",
            "InitFunc": "ExportMapping",
            "ConfPath": "fs1.conf"
        },
        "fs2": {
            "Type": "Backend",
            "PluginName": "fs2",
            "InitFunc": "ExportMapping",
            "ConfPath": "fs2.conf"
        },
        "localfs": {
            "Type": "LocalConf",
            "PluginName": "localfs",
            "InitFunc": "ExportMapping",
            "ConfPath": "localfs.conf"
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
}'
