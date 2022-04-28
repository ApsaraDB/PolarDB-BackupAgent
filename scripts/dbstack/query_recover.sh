# file name: query_recover.sh
# 执行方式： sh query_recover.sh ${clusterInsName} ${rwIp}
clusterInsName=$1
# clusterInsName='pc-xxxxxxxxxxxxxxxxxx';
rwIp=$2

curl "http://$rwIp:1888/manager/Describe" -d '{
"InstanceID": "'$clusterInsName'",
"BackupType": "Recovery"
}'