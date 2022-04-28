# file name: start_recover_data.sh
# 执行方式： sh start_recover_data.sh ${clusterInsName} ${recoveryTime} ${rwIp} ${rwDataVolumePath} ${httpEndpoint} ${encryptionPassword}

clusterInsName=$1
# clusterInsName='pc-xxxxxxxxxxxxxxxxx';
recoveryTime=$2
# recover time 为时间戳， 如： 1656503714
rwIp=$3
rwDataVolumePath=$4
httpEndpoint=$5
encryptionPassword=$6

storageGatewayInfoOutput=$(kubectl -n dbstack-dbaas  get svc storage-gateway | grep -v NAME)
storageGatewayInfo=(${storageGatewayInfoOutput// / })
gateWayIp=${storageGatewayInfo[2]}
gateWayPort=$(echo ${storageGatewayInfo[4]} | sed -r 's/([0-9]{1,}).*/\1/')

curl "http://$rwIp:1888/manager/Recovery" -d '{
  "BackupMachineList": ["http://'$rwIp':1888"],
  "InstanceID": "'$clusterInsName'",
  "BackupJobID": "default",
  "BackupMetaSource": "fs",
  "RecoveryTime": '$recoveryTime',
  "Full": {"InstanceID": "'$clusterInsName'"},
  "Incremental": {"InstanceID": "'$clusterInsName'", "BackupID": "increbk"},
  "Filesystem":"fs",
  "EnableEncryption": true,
  "EncryptionPassword": "'$encryptionPassword'",
  "BackupStorageSpace": {"StorageType":"http", "Locations":{"http": {"Endpoint": "'$httpEndpoint'"}}},
  "PGType": "PolarDBFlex",
  "RecoveryFolder": "'$rwDataVolumePath'/polardata",
  "DBClusterMetaDir":"'$rwDataVolumePath'/data"
}'