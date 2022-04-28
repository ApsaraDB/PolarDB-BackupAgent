#!/bin/sh

V=$(cat VERSION |grep Version |awk '{print $2}'|cut -d '.' -f 1)
V_=$(cat VERSION |grep Version |awk '{print $2}'|cut -d '.' -f 2)
V__=$(cat VERSION |grep Version |awk '{print $2}'|cut -d '.' -f 3)

commit_id=$(git rev-parse HEAD)
echo "commitId: $commit_id"

commit_branch=$(git symbolic-ref --short -q HEAD)
echo "branch $commit_branch"

commit_date=$(git log -1 --format="%cd")
echo "git commit date: $commit_date"


gitrepo=$(git remote -v|grep origin|grep fetch|awk '{print $2}')
echo "git repo: $gitrepo"

buildUser=$(whoami)
echo "buildUser: $buildUser"

BUILDDATE=$(date '+%Y%m%d%H%M%S')

buildHost=$(hostname)
echo "buildHost: $buildHost"

rm -f loader/version.go
echo "package main" > loader/version.go
echo "" >> loader/version.go
echo "const GitBranch = \"$commit_branch\"" >> loader/version.go
echo "const GitCommitId = \"$commit_id\"" >> loader/version.go
echo "const GitCommitDate = \"$commit_date\"" >> loader/version.go
echo "const GitCommitRepo = \"$gitrepo\"" >> loader/version.go
echo "const BuildDate = \"$BUILDDATE\"" >> loader/version.go
echo "const BuildUser = \"$buildUser\"" >> loader/version.go
echo "const BuildHost = \"$buildHost\"" >> loader/version.go
echo "const Version = \"v$V.$V_.$V__\"" >> loader/version.go
