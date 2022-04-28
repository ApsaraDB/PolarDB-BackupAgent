#!/bin/bash

. /etc/rc.d/init.d/functions

EXE=$1
echo $EXE

cur_dir=$(cd "$(dirname "$0")"; pwd)
exe_path=${cur_dir}/$EXE
up_dir=$(dirname $cur_dir)
#log_path=${up_dir}/log/
#lib_path=${up_dir}/lib/
#conf_name=`cd ${up_dir}/conf; ls | grep -E "log_agent\.conf$" | head -1; cd - > /dev/null 2>&1`
conf_path=${up_dir}/conf/${conf_name}
#arg="-arg_config_path=${conf_path} -arg_log_dir=${log_path}"

#export LD_LIBRARY_PATH=${lib_path}
export PATH=$PATH:/usr/local/bin
ulimit -c 102400

END=0
CNT=0
CORECNT=0
MAX_CORE_RETRY=100

while [ $END == 0 ]
do
	echo `date`@`hostname`
	echo $EXE
	#pushd $up_dir
    cd /usr/local/polardb_o_backup_tool_current/bin
	$EXE &
	#popd

	pid=$(jobs -p)
	wait $pid

	ret=$?
	echo "ret from child wait:$ret"
	CNT=$(($CNT+1))
	if [ $ret != 0 ] ; then
		if [ $ret == 1 ]; then
			echo "internal error, restart again"
			END=0
		#omit kill -9
		elif [ $ret != 137 ] && [ $ret > 128 ]; then
			CORECNT=$(($CORECNT+1))
			if [ $CORECNT -ge $MAX_CORE_RETRY ]; then
				echo "core over $MAX_CORE_RETRY times, exit"
				END=1
			fi
		fi
	fi

	if [ $END == 1 ]; then
		echo "supervisor exit"
	else
		sleep 5
		echo "child exited, respawn $CNT times"
	fi
done
