# file name: start_increment_log_backup.sh
# 执行方式： sh start_increment_log_backup.sh ${clusterInsName} ${httpEndpoint} ${encryptionPassword}

clusterInsName=$1
# clusterInsName='pc-xxxxxxxxxxxxxxxxx';
httpEndpoint=$2
encryptionPassword=$3

output=$(kubectl -ndbstack-dbaas get pod -l "apsara.cluster_ins.name=$clusterInsName,apsara.metric.db_type=polardb_flex_cm" -o custom-columns=NAME:.metadata.name,HOST_IP:.status.hostIP)
array=(${output// / })
if [ ${#array[@]} -lt 4 ]; then 
    echo "can't get $clusterInsName's cm pod"
fi
cmPodName=${array[2]}
cmHostIp=${array[3]}
cmPort=$(kubectl -ndbstack-dbaas get pod $cmPodName --show-labels | grep -v 'NAME' | sed -r 's/.*apsara.ins.port=([0-9]{1,}).*/\1/')
echo "CM_POD_NAME=$cmPodName"
echo "CM_HOST_IP=$cmHostIp"
echo "CM_PORT=$cmPort"

curl "http://$cmHostIp:$cmPort/manager/StartBackup" -d '{
  "InstanceID": "'$clusterInsName'",
  "BackupID": "increbk",
  "BackupType": "Incremental",
  "BackupJobID": "default",
  "Filesystem":"fs",
  "EnableEncryption": true,
  "EncryptionPassword": "'$encryptionPassword'",
  "BackupStorageSpace": {"StorageType":"http", "Locations":{"http": {"Endpoint": "'$httpEndpoint'"}}}
}'