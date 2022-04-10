#!/bin/bash
#
#  Sherlock Search Server Control Script
#  (c) 2002--2006 Martin Mares <mj@ucw.cz>
#  (c) 2006 Pavel Charvat <pchar@ucw.cz>
#

set -e

if [ $UID = 0 ] ; then
	cd ${SH_HOME:-~sherlock/run}
	exec su ${SH_USER:-sherlock} -c "exec bin/scontrol $@"
else
	if [ -n "$SH_HOME" ] ; then cd $SH_HOME ; fi
fi
if ! [ -f bin/sherlockd -a -f cf/sherlock ] ; then
	echo >&2 "scontrol: Cannot find Sherlock runtime directory"
	exit 1
fi
UCW_PROGNAME=scontrol
. lib/libucw.sh
parse-config "Search{#Port=8192; StatusFile}; !SKeeper{#TestRetry=10; #TestWait=3; CrashMail[]; RotateLogs; #CrashWaitThreshold; #CrashWaitCeiling; DaemonPIDFile; KeeperPIDFile; @TestQuery{#MinReplies; Query}; SwapLock; #SwapLockTimeout=10}"
SCONTROL="bin/scontrol$UCW_CF"

if [ $CF_SKeeper_CrashWaitThreshold -gt 0 ] ; then
	DHO="--pid-file=$CF_SKeeper_KeeperPIDFile"
	CMD=bin/skeeper
	MSG="sherlockd with skeeper"
else
	DHO="--pid-file=$CF_SKeeper_DaemonPIDFile"
	if [ -n "$CF_Search_StatusFile" ] ; then
		DHO="$DHO --status-file=$CF_Search_StatusFile"
	fi
	CMD=bin/sherlockd
	MSG="sherlockd"
fi

case "$1" in
	start)		echo -n "Starting $MSG..."
			shift
			# Bash is too weak for writing raceless startup/shutdown scripts,
			# so please forgive us a C helper.
			if bin/daemon-helper --check $DHO ; then
				echo " ALREADY RUNNING."
				exit 0
			fi
			bin/daemon-helper --start $DHO -- $CMD$UCW_CF "$@" &
			PID=$!
			RETRY=$CF_SKeeper_TestRetry
			echo -n "."
			while true ; do
				if [ -n "$CF_Search_StatusFile" -a ! -f "$CF_Search_StatusFile" ] ; then
					# sherlockd isn't initialized yet, so no point in trying queries
					true
				elif bin/query --host localhost --port $CF_Search_Port --silent --control '"databases"' >/dev/null 2>&1 ; then
					# sherlockd looks initialized, try sending a trivial query
					echo " done."
					exit 0
				fi
				if ! kill -0 $PID 2>/dev/null ; then
					# The daemon or daemon-helper has exited. Unfortunately, bash doesn't
					# give us any way how to get the return code, so in the rare case
					# sherlockd was already running and we haven't spotted it by --check,
					# we would report a failure incorrectly. Fortunately enough, if this
					# happens, the query check above exits with OK status first.
					echo " FAILED."
					exit 1
				fi
				if [ $RETRY = 0 ] ; then
					# The daemon got stuck, so go kill it
					echo " TIMED OUT."
					$SCONTROL stop
					exit 1
				fi
				RETRY=$(($RETRY-1))
				echo -n "."
				sleep $CF_SKeeper_TestWait
			done
			;;
	stop)		echo -n "Stopping $MSG... "
			if bin/daemon-helper --stop $DHO ; then
				if [ $CF_SKeeper_CrashWaitThreshold -gt 0 ] ; then
					# If we use the skeeper, kill skeeper first and then kill
					# the orphaned sherlockd, if there's any.
					bin/daemon-helper --stop --pid-file="$CF_SKeeper_DaemonPIDFile" || true
				fi
				if [ -n "$CF_Search_StatusFile" -a -f "$CF_Search_StatusFile" ] ; then
					rm -f "$CF_Search_StatusFile"
				fi
				echo "done."
			elif [ $? == 102 ] ; then
				echo "not running."
			else
				echo "FAILED."
				exit 1
			fi
			;;
	restart)	$SCONTROL stop
			$SCONTROL start
			;;
	cron)		if [ -n "$CF_SKeeper_RotateLogs" ] ; then eval "$CF_SKeeper_RotateLogs" ; fi
			;;
	test)		[ -z "${CF_SKeeper_TestQuery_Query[*]}" ] && exit 0
			echo -n "Testing sherlockd..."
			for index in `seq ${#CF_SKeeper_TestQuery_Query[*]}` ; do
				echo -n "."
				MINREPLIES=${CF_SKeeper_TestQuery_MinReplies[$index]}
				QUERY="${CF_SKeeper_TestQuery_Query[$index]}"
				T=tmp/test-$$
				if ! bin/query --host localhost --port $CF_Search_Port --noheader --nofooter "$QUERY" >$T ; then
					rm $T
					echo "FAILED."
					errlog "Query '$QUERY' failed."
					exit 1
				fi
				c=`grep -c "^B" $T || true`
				rm $T
				if [ $c -lt $MINREPLIES ] ; then
					echo "FAILED."
					errlog "Query '$QUERY' produced only $c replies ($MINREPLIES required)."
					exit 1
				fi
			done
			echo " done."
			;;
	swap)		shift
			INDICES=${*:-index}
			log "Swapping indices: $INDICES"
			ADEPTS=""
			for INDEX in $INDICES ; do
				if [ -f $INDEX.new/parameters ] ; then
					ADEPTS="$ADEPTS $INDEX"
				fi
			done
			if [ -z "$ADEPTS" ] ; then
				log "Nothing to swap"
				exit 0
			fi
			echo -n "Acquiring swap lock..."
			RETRY=$CF_SKeeper_SwapLockTimeout
			while ! mkdir "$CF_SKeeper_SwapLock" 2>/dev/null ; do
				if [ $RETRY = 0 ] ; then
					echo " TIMED OUT."
					exit 1
				fi
				RETRY=$(($RETRY-1))
				echo -n "."
				sleep 1
			done
			echo " done."
			RUN=
			if bin/daemon-helper --check $DHO ; then
				RUN=1
				$SCONTROL stop
			fi
			for INDEX in $ADEPTS ; do
				rm -rf $INDEX.old
				if [ -f $INDEX/parameters ] ; then
					mv $INDEX $INDEX.old
				else
					rm -rf $INDEX
				fi
				log "Swapping $INDEX"
				mv $INDEX.new $INDEX
			done
			if [ -z "$RUN" ] ; then
				rmdir "$CF_SKeeper_SwapLock"
				exit 0
			fi
			if $SCONTROL start ; then
				if $SCONTROL test ; then
					rmdir "$CF_SKeeper_SwapLock"
					exit 0
				fi
				$SCONTROL stop
			fi
			for INDEX in $ADEPTS ; do
				if [ -d $INDEX.old ] ; then
					log "Falling back to old $INDEX"
					mv $INDEX $INDEX.new
					mv $INDEX.old $INDEX
				else
					errlog "No old $INDEX to fall back to"
				fi
			done
			if $SCONTROL start ; then
				log "Fallback succeeded"
			else
				errlog "Fallback failed"
			fi
			rmdir "$CF_SKeeper_SwapLock"
			exit 1
			;;
	swap-blacklist)	shift
			INDEX="${1:-index}"
			BLACKLIST="${2:-blacklist}"
			SUFFIX="${3:-.new}"
			log "Swapping blacklist: $INDEX$SUFFIX/$BLACKLIST"
			if [ ! -f "$INDEX$SUFFIX/$BLACKLIST" -o ! -f "$INDEX$SUFFIX/parameters" ] ; then
				log "Nothing to swap"
				exit 0
			fi
			echo -n "Acquiring swap lock..."
			RETRY=$CF_SKeeper_SwapLockTimeout
			while ! mkdir "$CF_SKeeper_SwapLock" 2>/dev/null ; do
				if [ $RETRY = 0 ] ; then
					echo " TIMED OUT."
					exit 1
				fi
				RETRY=$(($RETRY-1))
				echo -n "."
				sleep 1
			done
			echo " done."
			RUN=
			if bin/daemon-helper --check $DHO ; then
				RUN=1
			fi
			echo -n "Checking blacklist version..."
			if [ "`bin/index-version $INDEX$SUFFIX`" != "`bin/index-version $INDEX`" ] ; then
				echo " INVALID."
				rmdir "$CF_SKeeper_SwapLock"
				exit 2
			fi
			echo " done."
			rm -rf "$INDEX/$BLACKLIST.old"
			if [ -f "$INDEX/$BLACKLIST" ] ; then
				mv "$INDEX/$BLACKLIST" "$INDEX/$BLACKLIST.old"
			fi
			log "Swapping $BLACKLIST"
			mv "$INDEX$SUFFIX/$BLACKLIST" "$INDEX/$BLACKLIST"
			if [ -z "$RUN" ] ; then
				rm -rf "$INDEX$SUFFIX"
				rmdir "$CF_SKeeper_SwapLock"
				exit 0
			fi
			if bin/sherlockd --merge-only ; then
				if $SCONTROL test ; then
					rm -rf "$INDEX$SUFFIX"
					rmdir "$CF_SKeeper_SwapLock"
					exit 0
				fi
			fi
			$SCONTROL stop
			if [ -f "$INDEX/$BLACKLIST.old" ] ; then
				log "Falling back to old $BLACKLIST"
				mv "$INDEX/$BLACKLIST" "$INDEX$SUFFIX/$BLACKLIST.new"
				mv "$INDEX/$BLACKLIST.old" "$INDEX/$BLACKLIST"
			else
				errlog "No old $BLACKLIST to fall back to"
			fi
			if $SCONTROL start ; then
				log "Fallback succeeded"
			else
				errlog "Fallback failed"
			fi
			rmdir "$CF_SKeeper_SwapLock"
			exit 1
			;;
	*)		echo >&2 "Usage: [SH_USER=<user>] [SH_HOME=<homedir>] scontrol (start|stop|restart|cron|test|swap|swap-blacklist [<index> | <index> <blacklist> <suffix>])"
			exit 1
			;;
esac
