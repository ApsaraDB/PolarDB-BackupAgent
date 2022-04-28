#!/bin/bash
V1=$(cat VERSION |grep Version |awk '{print $2}'|cut -d '.' -f 1)
V2=$(cat VERSION |grep Version |awk '{print $2}'|cut -d '.' -f 2)
V3=$(cat VERSION |grep Version |awk '{print $2}'|cut -d '.' -f 3)
echo $V3
V3NEW=$(($V3+1));
echo $V3NEW

CONTENT="Version: $V1.$V2.$V3NEW"
echo $CONTENT
echo $CONTENT > VERSION

sed -i "s/Version: .*/Version: $V1.$V2.$V3NEW/g" rpm/polardb-backup-agent.spec
sed -i "s/Version: .*/Version: $V1.$V2.$V3NEW/g" rpm/t-polardb-o-backupagent.spec

branch="update_version_$V1.$V2.$V3NEW"
git checkout -b $branch
git add VERSION
git add rpm/polardb-backup-agent.spec
git add rpm/t-polardb-o-backupagent.spec
git commit -m "$CONTENT"
git push origin $branch:$branch