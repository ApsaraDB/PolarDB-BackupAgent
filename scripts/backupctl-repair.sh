let numparam=$#
if [[ $numparam != 3 ]]
then
    echo "[ERROR] params are not valid, usage: ./backupctl-repair.sh \$endpoint \$port \$username"
    exit
fi
endpoint=$1
port=$2
username=$3
psql -c "select pg_stop_backup();" -h $endpoint -p $port -U $username
