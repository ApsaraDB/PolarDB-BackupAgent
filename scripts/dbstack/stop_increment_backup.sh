# file name: stop_increment_backup.sh
# 执行方式： sh stop_increment_backup.sh ${clusterInsName}

clusterInsName=$1
# clusterInsName='pc-xxxxxxxxxxxxxxxxx';
output=$(kubectl -ndbstack-dbaas get pod -l "apsara.cluster_ins.name=$clusterInsName,apsara.metric.db_type=polardb_flex_cm" -o custom-columns=NAME:.metadata.name,HOST_IP:.status.hostIP)
array=(${output// / })
if [ ${#array[@]} -lt 4 ]; then 
    echo "can't get $clusterInsName's cm pod"
fi
cmPodName=${array[2]}
cmHostIp=${array[3]}
cmPort=$(kubectl -ndbstack-dbaas get pod $cmPodName --show-labels | grep -v 'NAME' | sed -r 's/.*apsara.ins.port=([0-9]{1,}).*/\1/')

curl "http://$cmHostIp:$cmPort/manager/StopBackup" -d '{
  "InstanceID": "'$clusterInsName'",
  "BackupID": "increbk",
  "BackupType": "Incremental",
  "BackupJobID": "default"
}'