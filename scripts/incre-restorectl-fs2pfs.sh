#!/bin/bash

# User should config the following params
wals='["0000000A0000002100000002","0000000A0000002100000003"]'

# check size of params
let numparam=$#
if [[ $numparam != 3 ]]
then
    echo "[ERROR] params are not valid, usage: ./incre-restorectl-fs2pfs.sh \$src \$dst \$instanceid"
    exit 1
fi

# check params wals
if [ "$wals" == "" ]
then
    echo "[ERROR] user should config the param wals first"
    exit 1
fi

path=$1
loopname=$(echo $2 | cut -d '/' -f 2)
dbpath=/$(echo $2 | cut -d '/' -f 3)
instanceid=$3
./backup_ctl -instance $3 -plugin increpipeline -increpipeline '{
    "Compress": true,
    "WorkerCount": 2,
    "Action": "restore",
    "HostIP": "127.0.0.1",
    "Name": "increpipeline",
    "Force": false,
    "ManagerAddr": "127.0.0.1:1888",
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
}' -fs '{
    "Path": "'$path'"
}'