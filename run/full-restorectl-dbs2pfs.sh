#!/bin/bash
let numparam=$#
if [[ $numparam != 4 ]]
then
    echo "[ERROR] params are not valid, usage: ./full-restorectl-dbs2pfs.sh \$gatewaylist \$instanceid \$backupid \$dst"
    exit
fi

gatewaylist=$1
instanceid=$2
backupid=$3
loopname=$(echo $4 | cut -d '/' -f 2)
dbpath=/$(echo $4 | cut -d '/' -f 3)

./backup_ctl -instance $instanceid -plugin pgpipeline -dbs '{
    "InstanceID": "'$instanceid'",
    "GatewayList": ["'$gatewaylist'"],
    "BackupID": "'$backupid'",
    "Name": "dbs"
}' -pgpipeline '{
    "Compress": true,
    "WorkerCount": 16,
    "Action": "restore",
    "HostIP": "127.0.0.1",
    "Name": "pgpipeline",
    "Force": false,
    "ManagerAddr": ["127.0.0.1:1888"],
    "InstanceID": "'$instanceid'",
    "BackupID": "'$backupid'",
    "Frontend": "pfs",
    "Backend":"dbs",
    "Endpoints": {
        "pfs": {
            "Type": "Frontend",
            "PluginName": "pfs",
            "InitFunc": "ExportMapping",
            "ConfPath": "pfs.conf"
        },
        "dbs": {
            "Type": "Backend",
            "PluginName": "dbs",
            "InitFunc": "ExportMapping",
            "ConfPath": "dbs.conf"
        }
    },
    "DBHomePath": "",
    "StatusURL": "/manager/BackupStatus"
}' -pfs '{
    "Pbd": "'$loopname'",
    "Cluster": "disk",
    "Mode": "file",
    "Flags": "restore",
    "Name": "pfs",
    "DBPath": "'$dbpath'"
}'