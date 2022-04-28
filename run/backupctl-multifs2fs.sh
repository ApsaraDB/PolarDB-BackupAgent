#!/bin/bash
let numparam=$#
if [[ $numparam != 5 ]]
then
    echo "[ERROR] params are not valid, usage: ./backupctl-multifs2fs.sh \$polardata \$pgdata \$dst \$InstanceID \$BackupID"
    exit
fi
InstanceID=$4
BackupID=$5
./backup_ctl -instance "$InstanceID" -plugin pgpipeline -fs1 '{
    "Path": "'$1'"
}' -fs2 '{
    "Path": "'$3'",
    "InstanceID": "'$InstanceID'",
    "BackupID": "'$BackupID'"
}' -pgpipeline '{
    "Compress": true,
    "WorkerCount": 2,
    "Action": "backup",
    "HostIP": "127.0.0.1",
    "Name": "pgpipeline",
    "Force": false,
    "ManagerAddr": ["127.0.0.1:1888"],
    "Frontend": "fs1",
    "Backend":"fs2",
    "InstanceID":"'$InstanceID'",
    "BackupID":"'$BackupID'",
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
    "LocalFileDir": "'$2'"
}'