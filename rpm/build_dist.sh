#!/usr/bin/env bash
set -e

check_call()
{
    eval $@
    rc=$?
    if [[ ${rc} -ne 0 ]]; then
        echo "[$@] execute fail: $rc"
        exit 1
    fi
}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
PROJECT_ROOT=$(dirname ${SCRIPT_DIR})
cd ${PROJECT_ROOT}

#./3rd/build.sh

# ./3rd dir
GO_BASE=${PROJECT_ROOT}/3rd

# install_golang
get_arch=`arch`
if [[ $get_arch =~ "x86_64" ]];then
  echo "this is x86_64"
  tar zxf ${GO_BASE}/go1.14.4.linux-amd64.tar.gz -C ${GO_BASE}
elif [[ $get_arch =~ "aarch64" ]];then
  echo "this is arm64"
  tar zxf ${GO_BASE}/go1.14.4.linux-arm64.tar.gz -C ${GO_BASE}
else
  echo "error: not support arch type"
  exit 1
fi

git config --global url.git@gitlab.alibaba-inc.com:.insteadOf https://gitlab.alibaba-inc.com

export PATH=${GO_BASE}/go/bin:$PATH
export GOROOT=${GO_BASE}/go
export GOPRIVATE=gitlab.alibaba-inc.com
export GOPROXY=http://gomodule-repository.aone.alibaba-inc.com,https://proxy.golang.org,direct

go version

cd ${PROJECT_ROOT}

make all
