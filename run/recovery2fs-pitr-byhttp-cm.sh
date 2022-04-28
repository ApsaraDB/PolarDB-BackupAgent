#!/bin/bash -x

# User should config the following params
cmgateway=""
backupmachine=""
polardbdata=""
pgdata=""
dst=""
instanceid=""
fullbackupid=""
recoverytime=

# example
# cmgateway="xx.xx.xx.xx:5500"
# backupmachine="xx.xx.xx.xx:1888"
# polardbdata="/mnt/disk3"
# pgdata="/var/local/polardb/clusters/mycluster"
# dst="xx.xx.xx.xx:8080"
# instanceid="polar-dma-test"
# fullbackupid=""
# recoverytime=1616050150

##########################################

# execute recovery
curl "http://$cmgateway/manager/Recovery" -d '{
  "GatewayAddressList": [""],
  "BackupMachineList": ["http://'$backupmachine'"],
  "InstanceID": "'$instanceid'",
  "BackupJobID": "default",
  "CallbackURL": "http://127.0.0.1:1889",
  "BackupMetaSource": "fs",
  "RecoveryTime": '$recoverytime',
  "Full": {"InstanceID": "'$instanceid'", "BackupID": "'$fullbackupid'"},
  "Incremental": {"InstanceID": "'$instanceid'", "BackupID": "increbk"},
  "Filesystem":"fs",
  "RecoveryFolder": "'$polardbdata'",
  "BackupStorageSpace": {"StorageType":"http", "Locations":{"http": {"Endpoint": "'$dst'"}}},
  "PGType": "PolarDBFlex",
  "DBClusterMetaDir":"'$pgdata'"
}'