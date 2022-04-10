#!/bin/bash
#
#  Control Script for Sherlock's scheduler
#  (c) 2002--2004 Martin Mares <mj@ucw.cz>
#

set -e

DHO=${DHO:---pid-file=lock/scheduler.pid --status-file=lock/scheduler.slot}
SCHEDULER_NAME=${SCHEDULER_NAME:-Sherlock}

if [ $UID = 0 ] ; then
	cd ${SH_HOME:-~sherlock/run}
	exec su ${SH_USER:-sherlock} -c "exec bin/sched-control $@"
else
	if [ -n "$SH_HOME" ] ; then cd $SH_HOME ; fi
fi
if ! [ -f bin/scheduler -a -f cf/sherlock ] ; then
	echo >&2 "sched-control: Cannot find Sherlock runtime directory or bin/scheduler"
	exit 1
fi

case "$1" in
	start)		echo -n "Starting $SCHEDULER_NAME scheduler..."
			# Bash is too weak for writing raceless startup/shutdown scripts,
			# so please forgive us a C helper.
			if bin/daemon-helper --check $DHO ; then
				echo " ALREADY RUNNING."
				exit 0
			fi
			shift
			bin/daemon-helper --start $DHO -- bin/scheduler "$@" &
			echo " done."
			;;
	stop)		echo -n "Stopping $SCHEDULER_NAME scheduler... "
			if bin/daemon-helper --stop $DHO ; then
				echo "done."
			elif [ $? == 102 ] ; then
				echo "not running."
			else
				echo "FAILED."
				exit 1
			fi
			;;
	restart)	shift
			bin/sched-control stop $@
			bin/sched-control start $@
			;;
	*)		echo >&2 "Usage: [SH_USER=<user>] [SH_HOME=<homedir>] sched-control (start [<initial-slot>|<timetable>]|stop|restart)"
			exit 1
			;;
esac
