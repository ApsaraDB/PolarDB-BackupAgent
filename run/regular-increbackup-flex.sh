#!/bin/bash -x

# User should config the following params
cmgateway=""
polardbdata=""
pgdata=""
storagetype=""
dst=""
instanceid=""

# example
# cmgateway="xx.xx.xx.xx:5500"
# polardbdata="/mnt/disk2"
# pgdata="/var/local/polardb/clusters/mycluster"
# storagetype="http"
# dst="xx.xx.xx.xx:8080"
# instanceid="polar-dma-test"

##########################################

# generate backupid
backupid="increbk"

# execute backup
curl "http://$cmgateway/manager/StartBackup" -d '{
  "InstanceID": "'$instanceid'",
  "BackupID": "'$backupid'",
  "BackupType": "Incremental",
  "BackupJobID": "default",
  "Filesystem":"fs",
  "BackupStorageSpace": {"StorageType":"'$storagetype'", "Locations":{"http": {"Endpoint": "'$dst'"}, "Local": {"Path": "'$dst'"}}},
  "BackupFolder": "'$polardbdata'",
  "DBClusterMetaDir":"'$pgdata'",
  "PGType": "PolarDBFlex"
}'