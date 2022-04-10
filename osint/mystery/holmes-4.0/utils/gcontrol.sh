#!/bin/bash
#
#  Sherlock Gatherer Control Script
#  (c) 2002--2004 Martin Mares <mj@ucw.cz>
#

set -e

if [ $UID = 0 ] ; then
	cd ${SH_HOME:-~sherlock/run}
	exec su ${SH_USER:-sherlock} -c "exec bin/gcontrol $@"
else
	if [ -n "$SH_HOME" ] ; then cd $SH_HOME ; fi
fi
if ! [ -f bin/daemon-helper -a -f cf/sherlock ] ; then
	echo >&2 "gcontrol: Cannot find Sherlock runtime directory"
	exit 1
fi
GCONTROL="bin/gcontrol$UCW_CF"

#ifdef CONFIG_SHEPHERD
DHO="--pid-file=lock/shepherd.pid"
NAME=shepherd
#else
DHO="--pid-file=lock/scheduler.pid --status-file=lock/scheduler.slot"
NAME=scheduler
#endif"

case "$1" in
	start)		echo -n "Starting $NAME..."
			# Bash is too weak for writing raceless startup/shutdown scripts,
			# so please forgive us a C helper.
			if bin/daemon-helper --check $DHO ; then
				echo " ALREADY RUNNING."
				exit 0
			fi
			shift
			bin/daemon-helper --start $DHO -- bin/$NAME "$@" &
			echo " done."
			;;
	stop|force-stop)
			echo -n "Stopping $NAME... "
			if bin/daemon-helper --$1 $DHO ; then
				echo "done."
			elif [ $? == 102 ] ; then
				echo "not running."
			else
				echo "FAILED."
				exit 1
			fi
			;;
#ifdef CONFIG_SHEPHERD
	reload)		echo -n "Reloading $NAME... "
			bin/shepherd --check-config
			if bin/daemon-helper --reload $DHO ; then
				echo "done."
			elif [ $? == 102 ] ; then
				echo "not running."
			else
				echo "FAILED."
				exit 1
			fi
			;;
#else
	reload)		$GCONTROL stop
			$GCONTROL start
			;;
#endif
	restart)	$GCONTROL stop
			$GCONTROL start
			;;
	*)		echo >&2 "Usage: [SH_USER=<user>] [SH_HOME=<homedir>] gcontrol (start [<initial-slot>]|stop|force-stop|restart|reload)"
			exit 1
			;;
esac
