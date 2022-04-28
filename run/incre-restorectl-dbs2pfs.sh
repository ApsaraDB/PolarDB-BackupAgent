#!/bin/bash

# User should config the following params
wals='["0000000C0000002400000000","0000000C0000002400000001"]'

let numparam=$#
if [[ $numparam != 4 ]]
then
    echo "[ERROR] params are not valid, usage: ./incre-restorectl-dbs2pfs.sh \$gatewaylist \$instanceid \$backupid \$dst"
    exit
fi

gatewaylist=$1
instanceid=$2
backupid=$3
loopname=$(echo $4 | cut -d '/' -f 2)
dbpath=/$(echo $4 | cut -d '/' -f 3)

./backup_ctl -instance $instanceid -plugin increpipeline -increpipeline '{
    "Compress": true,
    "RealTime": false,
    "WorkerCount": 16,
    "Action": "restore",
    "HostIP": "127.0.0.1",
    "Name": "increpipeline",
    "Force": false,
    "ManagerAddr": ["127.0.0.1:1888"],
    "InstanceID": "'$instanceid'",
    "BackupID": "'$backupid'",
    "Frontend": "pfs",
    "Backend":"dbs",
    "Endpoints": {
        "Frontend": {
            "Name": "Frontend",
            "PluginName": "pfs",
            "InitFunc": "ExportMapping",
            "ConfPath": "pfs.conf"
        },
        "Backend": {
            "Name": "Backend",
            "PluginName": "dbs",
            "InitFunc": "ExportMapping",
            "ConfPath": "dbs.conf"
        }
    },
    "RecoveryWals": '$wals',
    "DBHomePath": "",
    "StatusURL": "/manager/BackupStatus"
}' -pfs '{
    "Pbd": "'$loopname'",
    "Cluster": "disk",
    "Mode": "file",
    "Flags": "restore",
    "Name": "pfs",
    "DBPath": "'$dbpath'"
}' -dbs '{
    "GatewayList": ["'$gatewaylist'"],
    "InstanceID": "'$instanceid'",
    "BackupID": "'$backupid'",
    "Name": "dbs"
}'