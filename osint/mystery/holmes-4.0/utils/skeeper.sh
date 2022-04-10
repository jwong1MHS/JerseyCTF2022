#!/bin/bash
#
#  Sherlock Search Server Keeper Script
#  (c) 2002--2005 Martin Mares <mj@ucw.cz>
#  (c) 2006 Pavel Charvat <pchar@ucw.cz>
#

ulimit -c unlimited

if [ "$UID" = 0 ] ; then
	echo >&2 "skeeper: Must not be run as root!"
	exit 1
fi
if [ "$1" == start -o "$1" == stop -o "$1" == restart ] ; then
	echo >&2 "skeeper: You probably meant scontrol $@, didn't you?"
	exit 1
fi
UCW_PROGNAME=skeeper
. lib/libucw.sh
parse-config "Search{#Port=8192; StatusFile}; !SKeeper{#TestRetry=10; #TestWait=3; CrashMail[]; RotateLogs; #CrashWaitThreshold; #CrashWaitCeiling; DaemonPIDFile; KeeperPIDFile; @TestQuery{#MinReplies; Query}; SwapLock; #SwapLockTimeout=10}"

DHO="--pid-file=$CF_SKeeper_DaemonPIDFile"
if [ -n "$CF_Search_StatusFile" ] ; then
	DHO="$DHO --status-file=$CF_Search_StatusFile"
fi

exec </dev/null >/dev/null 2>&1

function send_report
{
	if [ -n "${CF_SKeeper_CrashMail[*]}" ] ; then
		(
			cat <<EOF
With sorrow in our hearts, we announce that sherlockd $@.

Host:			`hostname -f`
Current directory:	`pwd`
Date and time:		`date`
Last words:
EOF
			tail -10 `ls -1t log/sherlockd* | head -1`
		) | bin/send-mail -s "sherlockd $*" "${CF_SKeeper_CrashMail[@]}"
	fi
}

leow=0
while true ; do
	bin/daemon-helper --start $DHO -- `pwd`/bin/sherlockd$UCW_CF "$@"
	RC=$?
	[ $RC = 101 ] && exit 0  # Already running
	if [ -n "$CF_Search_StatusFile" -a ! -s "$CF_Search_StatusFile" ] ; then
		send_report "failed to start"
		exit 1
	else
		send_report "exited with return code $RC"
	fi
	now=`date +%s`
	if [ $(($now-$leow)) -gt $CF_SKeeper_CrashWaitThreshold ] ; then
		delay=1
	elif [ $delay -lt $CF_SKeeper_CrashWaitCeiling ] ; then
		delay=$((2*$delay))
	fi
	sleep $delay
	leow=$now
done
