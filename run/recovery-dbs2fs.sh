#!/bin/bash -x

# User should config the following params
backupmachine=""
gatewaylist=""
namespace="testbk"
polardbdata=""
instanceid=""
fullbackupid=""
recoverytime=1656503714

# example
# backupmachine="xx.xx.xx.xx:1888"
# polardbdata="/u01/data_r"
# gatewaylist="127.0.0.1:9997"
# instanceid="polar-dma-test"
# fullbackupid=""
# recoverytime=1616050150

##########################################

# execute recovery
curl "http://$backupmachine/manager/Recovery" -d '{
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
  "BackupStorageSpace": {"StorageType":"dbs", "Locations":{"DBS": {"Endpoint": "'$gatewaylist'", "Namespace": "'$namespace'"}}},
  "PGType": "PolarDBFlex"
}'