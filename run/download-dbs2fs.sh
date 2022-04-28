#!/bin/bash

let numparam=$#
if [[ $numparam != 5 ]]
then
    echo "[ERROR] params are not valid, usage: ./download-dbs2pfs.sh \$gatewaylist \$instanceid \$backupid \$file \$dst"
    exit
fi

gatewaylist=$1
instanceid=$2
backupid=$3
file=$4
dst=$5

mkdir $dst/pg_wal

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
    "RecoveryWals": ["'$file'"],
    "DBHomePath": "",
    "StatusURL": "/manager/BackupStatus"
}' -fs '{
    "Path": "'$dst'"
}' -dbs '{
    "GatewayList": ["'$gatewaylist'"],
    "InstanceID": "'$instanceid'",
    "BackupID": "'$backupid'",
    "Name": "dbs"
}'

if [ $? -eq 0 ]
then
    mv $dst/pg_wal/$file $dst
    rm -r $dst/pg_wal
    echo "[INFO] download $file to $dst success"
else
    echo "[ERROR] download faild"
fi