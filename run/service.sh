#!/bin/bash

###############
# universe 
# chkconfig: - 99 15
# description: RDS universe is the script for starting RDS universe on boot.
### BEGIN INIT INFO
# Provides: node
# Required-Start:    $network $remote_fs $local_fs
# Required-Stop:     $network $remote_fs $local_fs
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: start and stop RDS universe
# Description: RDS universe
### END INIT INFO

###############

. /etc/rc.d/init.d/functions

EXE="/usr/local/polardb_o_backup_tool_current/bin/backup_ctl"
slink=$(readlink -f "$0")
if [ "x$slink" == "x" ] ; then
	slink=$0
fi
cur_dir=$(cd "$(dirname $slink)"; pwd)
res_log=${cur_dir}/../log/r.log
RETVAL=0

checkenv()
{
	echo "check env"
}

stop_universe()
{
	pkill supervisor.sh
	if [ "x$(pidofproc $EXE)" != "x" ]; then
		kill -15 $(pidofproc $EXE)
		sleep 5
		kill -9 $(pidofproc $EXE)
	fi
}

supervisor()
{
	ulimit -n 131072
	ulimit -c unlimited
	nohup ${cur_dir}/supervisor.sh $EXE >> $res_log 2>&1 </dev/null &
}

start()
{
    if [ "x$(pidofproc $EXE)" != "x" ] ; then
        echo -n "$EXE is running, stop it first"
        success $""
        echo ""
        RETVAL=0
        return 0
    fi

	echo -n $"Start " $EXE

	supervisor
	sleep 1

    if [ "x$(pidofproc supervisor.sh)" != "x" -a  "x$(pidofproc $EXE)" != "x" ]; then
        success $""
        RETVAL=0
    else
        failure $""
        RETVAL=-1
    fi
	echo
}

stop()
{
    echo -n $"Stopping " $EXE

    stop_universe

	END=0
	CNT=0
	while [ $END == 0 ]
	do
		if [ "x$(pidofproc supervisor.sh)" != "x" -o  "x$(pidofproc $EXE)" != "x" ] ; then
			sleep 1

			CNT=$(($CNT+1))
			if [ $CNT -ge 15 ]; then
				stop_universe
				echo -n " stop for $CNT seconds, retry to kill"
			fi

			if [ $CNT -ge 30 ]; then
				echo -n " stop timeout for 30 seconds"
				END=1
			fi
		else
			END=1
		fi
	done

	if [ "x$(pidofproc supervisor.sh)" != "x" -o  "x$(pidofproc $EXE)" != "x" ] ; then
		failure $""
		RETVAL=-1
	else
		success $""
		RETVAL=0
	fi
	echo
}

restart()
{
	echo -n $"Restart " $EXE
	stop
	start
}

status()
{
    if [ "x$(pidofproc supervisor.sh)" != "x" -a  "x$(pidofproc $EXE)" != "x" ] ; then
        echo -n "All universe process is running"
        success $""
        echo ""
		RETVAL=0
        return 0
    fi

    if [ "x$(pidofproc supervisor.sh)" == "x" ] ; then
        echo -n "supervisor.sh is not running"
        failure $""
        echo ""
		RETVAL=-1
    else
        echo -n "supervisor.sh is running"
        success $""
        echo ""
		RETVAL=0
    fi

    if [ "x$(pidofproc $EXE)" == "x" ] ; then
        echo -n "$EXE is not running"
        failure $""
        echo ""
		RETVAL=-1
    else
        echo -n "$EXE is running"
        success $""
        echo ""
		RETVAL=0
    fi
}

check()
{
	status
}

case "$1" in
    start|stop|restart|status|check)
        $1
        ;;
    *)
        echo $"Usage: $0 {start|stop|status|restart|check}"
        exit 1
        ;;
esac
exit $RETVAL