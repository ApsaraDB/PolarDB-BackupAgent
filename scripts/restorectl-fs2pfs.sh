#!/bin/bash
echo "src: $1"
echo "dst: $2"
path=$1
loopname=$(echo $2 | cut -d '/' -f 2)
dbpath=/$(echo $2 | cut -d '/' -f 3)
./backup_ctl -plugin pgpipeline -fs '{
    "Path": "'$path'"
}' -pgpipeline '{
    "Compress": true,
    "WorkerCount": 2,
    "Action": "restore",
    "HostIP": "127.0.0.1",
    "Name": "pgpipeline",
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