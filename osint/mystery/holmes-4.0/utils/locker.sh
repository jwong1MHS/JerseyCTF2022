#!/bin/bash
#
# Sherlock's script for atomic commands execution
#
# (c) 2006 Pavel Charvat <pchar@ucw.cz>

function usage
{
	if [ -n "$*" ] ; then
		echo "Error: $*"
		echo
	fi
	cat >&2 <<EOF
Usage: bin/locker [options] (acquire | release | <commands>)

Options:
  -s                Silent mode (disable "log" command)
  -d                Display debug messages (enable "dbg" command)
  -l <dir>          Lock path (default="lock/locker")
  -t <num>          Lock timeout in second (default=0=infinity)
  -p <string>       Prefix to temporary files (default="tmp/locker-")

Special non-bash commands:
  log <message>     Display a message
  dbg <message>     Display a debug message
  err <message>     Display an error
  die <message>     Display an error and exit with code 1
  cmd <cmd>         Equivalent to "dbg <cmd> ; <cmd>"
  save <cmd>        Execute this "rollback" command if something fails
  new-temp <var>    Allocates an unique file name (automatically deleted when leaving)
EOF
	exit 1
}

LOCKER_SILENT=
LOCKER_DEBUG=
LOCKER_LOCK="lock/locker"
LOCKER_TIMEOUT=0
LOCKER_TEMP_PREFIX="tmp/locker-"

# Messages

function log
{
	if [ -z "$LOCKER_SILENT" ] ; then
		bin/logger locker I "$*"
	fi
}

function dbg
{
	if [ -n "$LOCKER_DEBUG" ] ; then
		bin/logger locker D "$*"
	fi
}

function err
{
	bin/logger locker ! "$*"
}

function die
{
	err "$*"
	exit 1
}

# Acquiring and releasing of the global lock

function acquire
(
	log "Acquiring lock '$LOCKER_LOCK'"
	RETRY=$LOCKER_TIMEOUT
	while ! mkdir "$LOCKER_LOCK" 2>/dev/null ; do
		if [ $LOCKER_TIMEOUT -gt 0 ] ; then
			if [ $RETRY = 0 ] ; then
				die "Timed out"
			fi
			RETRY=$(($RETRY-1))
		fi
		sleep 1
	done
	dbg "Lock acquired"
)

function release
{
	dbg "Deleting temporary files"
	rm -rf "$LOCKER_TEMP_PREFIX"*
	log "Releasing lock"
	rmdir "$LOCKER_LOCK"
}

# Rollbacks (can save commands with any string parameters)

unset ${!LOCKER_ROLLBACK_*}
LOCKER_ROLLBACK_COUNT=0

function save
{
	for X in "$@" ; do
		eval "LOCKER_ROLLBACK_${LOCKER_ROLLBACK_COUNT}[\${#LOCKER_ROLLBACK_$LOCKER_ROLLBACK_COUNT[@]}]="'"$X"'
	done
	LOCKER_ROLLBACK_COUNT=$((1+$LOCKER_ROLLBACK_COUNT))
}

function rollback
{
	LOCKER_EXIT_CODE=$?
	trap - EXIT
	if [ $LOCKER_ROLLBACK_COUNT -gt 0 ] ; then
		err "Rolling back"
		for X in `seq $(($LOCKER_ROLLBACK_COUNT-1)) -1 0` ; do
			eval dbg "Rolling back \\'\${LOCKER_ROLLBACK_$X[*]}\\'"
			eval "\"\${LOCKER_ROLLBACK_$X[@]}\""
		done
	fi
	release
	dbg "Exitting with code $LOCKER_EXIT_CODE"
	exit $LOCKER_EXIT_CODE
}

# Useful commands

function cmd
{
	dbg "Running '$*'"
	"$@" || rollback
}

LOCKER_TEMP_COUNT=0

function new-temp
{
	LOCKER_TEMP_COUNT=$((1+$LOCKER_TEMP_COUNT))
	eval "$1=\"\$LOCKER_TEMP_PREFIX\$LOCKER_TEMP_COUNT\""
}

# ... and useless ones :-)

function safe-rm
{
	dbg "Running 'safe-rm $*'"
	for LOCKER_X in "$@" ; do
		if [ -e "$LOCKER_X" ] ; then
			new-temp LOCKER_Y
			mv -f "$LOCKER_X" "$LOCKER_Y"
			save mv -f "$LOCKER_Y" "$LOCKER_X"
		fi
	done
}

# Parse parameters

set -e
while getopts "sdl:t:p:" LOCKER_OPT ; do
	case "$LOCKER_OPT" in
		s)	LOCKER_SILENT=1
			;;
		d)	LOCKER_DEBUG=1
			;;
		l)	LOCKER_LOCK="$OPTARG"
			;;
		t)	LOCKER_TIMEOUT=$OPTARG
			;;
		p)	LOCKER_TEMP_PREFIX="$OPTARG"
			;;
		*)	usage "Invalid option"
			;;
	esac
done
shift $(($OPTIND - 1))
if [ -z "$*" ] ; then
	usage "Empty command"
fi

# Execute commands

if [ "$*" = acquire ] ; then
	acquire
elif [ "$*" = release ] ; then
	release
else
	acquire
	trap rollback EXIT
	eval "$*"
	trap - EXIT
	release
fi
