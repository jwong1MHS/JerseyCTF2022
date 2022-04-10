#!/bin/bash
#
#  Sherlock Asynchronous Blacklist Control Script
#  (c) 2006 Pavel Charvat <pchar@ucw.cz>
#
#
#  Update schema for a new "index", which overrides "under" indices and is overriden by "over" indices:
#
#   1. upload "index"
#   2. switch new "index" to a fixed shared directory (atomic)
#   3. clone shared "over" directories to temporary ones (atomic for each index)
#   4. generate blacklists for "index" using the cloned card-prints of "over" indices
#   5. upload blacklists and switch "index" (atomic switch for each index)
#
#   6. clone shared "under" directories to temporary ones (atomic for each index)
#   7. generate blacklists for "under" indices in their temporary directories
#   8. upload blacklists for "under" indices
#   9. switch blacklists for "under" indices (atomic for each index)
#      if failed because of invalid "under" index version, goto 6. and try again
#
#  10. restart schedulers for "under" indices if they has been changed (rare)

set -e

IN_SSH_AGENT=
SSH_OPTIONS=
SHCP_OPTIONS=
if [ "$1" == "--have-agent" ] ; then
	ssh-add ~/.ssh/send-index-key
	IN_SSH_AGENT=1
	SSH_OPTIONS="-A"
	SHCP_OPTIONS="-a -A"
	shift
fi

function usage
{
	cat >&2 <<EOF
Usage: black-control [options] <command>

Commands:
swap-dirs <dirs...>                       Atomic switch <dir> -> <dir>.old, <dir><suffix> -> <dir>
clone-dirs <dirs...>                      Atomic hard-link <dir>/<shared-files> -> <dir><suffix>/
upload-blacklist <index> <servers...>     Upload <index>/{parameters,blacklist}
swap-blacklist <servers...>               Switch uploaded blacklist
update-loop <indices> <dir> <servers...>

Options:
--lock-name=...         Switch/clone lock file (default is "lock/black-control")
--lock-timeout=...      Switch/clone lock timeout
--shared-files=...	Clone only selected files (if exist)
--shared-server=...     Server with shared directories (default is "")
--shared-homedir=...    Server homedir instead of default "run" (ignored with empty shared-server)
--remote-homedir=...    Use ... as remote homedir instead of default "run"
--remote-index-name=... Use ... as remote index directory instead of default "index"
--direct                Use direct IO (uncached)
--limit=...             Transfer speed limit (in MB/s)
--agent                 Start ssh-agent and use it for remote authentication
--update-retry=...      Update-loop retry count (default is infinity)
--update-delay=...      Update-loop retry delay
--suffix=...            Clone/temporary directories suffix (default is ".black")
--blacklist=...         Blacklist name (default is "blacklist")
--swap-delay=...        Blacklist swap delay

Note: If the --agent switch is used, it must precede all other switches.
EOF
	exit 1;
}

LOCK="lock/black-control"
LOCK_TIMEOUT=120
SHARED_FILES="*"
SERVER=
SHARED_HOMEDIR=run
REMOTE_HOMEDIR=run
REMOTE_INDEX_NAME=index
DIRECT=
LIMIT=
UPDATE_RETRY=1000000
UPDATE_DELAY=120
SUFFIX=.black
SEND_INDICES_OPTS=""
BLACKLIST=blacklist
SWAP_DELAY=5

while [ "${1:0:2}" == "--" ] ; do
	case "$1" in
		--lock-name=*)		LOCK="${1:12}"
					;;
		--lock-timeout=*)	LOCK_TIMEOUT="${1:15}"
					;;
		--shared-server=*)	SERVER="${1:16}"
					;;
		--shared-homedir=*)	SHARED_HOMEDIR="${1:17}"
					;;
		--shared-files=*)	SHARED_FILES="${1:15}"
					;;
		--remote-homedir=*)	REMOTE_HOMEDIR=${1:17}
					SEND_INDICES_OPTS="$SEND_INDICES_OPTS $1"
					;;
		--remote-index-name=*)	REMOTE_INDEX_NAME=${1:20}
					;;
		--direct)		DIRECT='-d'
					SEND_INDICES_OPTS="$SEND_INDICES_OPTS $1"
					;;
                --limit=*)		LIMIT="-l ${1:8}"
					SEND_INDICES_OPTS="$SEND_INDICES_OPTS $1"
					;;
		--agent)		[ -z "$IN_SSH_AGENT" ] && exec ssh-agent "$0" --have-agent "$@"
					SEND_INDICES_OPTS="$SEND_INDICES_OPTS $1"
					;;
		--update-retry=*)	UPDATE_RETRY="${1:15}"
					;;
		--update-delay=*)	UPDATE_DELAY="${1:15}"
					;;
		--suffix=*)		SUFFIX="${1:9}"
					;;
		--blacklist=*)		BLACKLIST="${1:12}"
					;;
		--swap-delay=*)		SWAP_DELAY="${1:13}"
					;;
		*)			usage
					;;
	esac
	shift
done

function log
{
        bin/logger black-control I "$1"
}

function debug
{
        bin/logger black-control D "$1"
}

function warn
{
        bin/logger black-control W "$1"
}

function die
{
        bin/logger black-control ! "$1"
        exit 1
}

function cmd
{
        debug "Sending to $1: set -e ; cd $REMOTE_HOMEDIR ; $2"
        ssh $SSH_OPTIONS $1 "set -e ; cd $REMOTE_HOMEDIR ; $2"
}

function acquire_lock
{
	log "Acquiring lock"
	RETRY=$LOCK_TIMEOUT
	while ! mkdir "$LOCK" 2>/dev/null ; do
		[ $RETRY = 0 ] && die "Timed out"
		RETRY=$(($RETRY-1))
		sleep 1
	done
}

function swap_dirs
{
	for DIR in "$@" ; do
		for FILE in "$DIR$SUFFIX"/* ; do
			[ -f "$FILE" ] || die "Directory must contain only regular files"
		done
		rm -rf "$DIR.old"
	done
	acquire_lock
	log "Switching directories"
	for DIR in "$@" ; do
		if [ -e "$DIR" ] ; then
			mv "$DIR" "$DIR.old"
		fi
		mv "$DIR$SUFFIX" "$DIR"
	done
	rmdir "$LOCK"
	log "Directories switched"
}

function clone_dirs
{
	for DIR in "$@" ; do
		rm -rf "$DIR$SUFFIX"
		mkdir -p "$DIR$SUFFIX"
	done
	if [ -n "$SERVER" ] ; then
		ssh $SSH_OPTIONS $SERVER "set -e ; cd $SHARED_HOMEDIR ; bin/black-control --lock-name=$LOCK --lock-timeout=$LOCK_TIMEOUT --suffix=$SUFFIX --shared-files=\"$SHARED_FILES\" clone-dirs $*"
		log "Copying remote directories"
		for DIR in "$@" ; do
			SRC="`ssh $SSH_OPTIONS $SERVER "set -e ; cd $SHARED_HOMEDIR ; echo $DIR$SUFFIX/* "`"
			SRC2=
			for S in $SRC ; do
				[ "${S:$((${#S}-1)):1}" = '*' ] || SRC2="$SRC2 $SERVER:$S"
			done
			[ -n "$SRC2" ] && bin/shcp $SHCP_OPTIONS $DIRECT $LIMIT -e $SHARED_HOMEDIR { $SRC2 } "$DIR$SUFFIX"
			ssh $SSH_OPTIONS $SERVER "rm -rf $SHARED_HOMEDIR/$DIR$SUFFIX/"
		done
	else
		acquire_lock
		log "Cloning directories"
		for DIR in "$@" ; do
		  	if [ -e "$DIR" ] ; then
				for FILE in `(cd "$DIR" ; eval echo "$SHARED_FILES")` ; do
					if [ -f "$DIR/$FILE" ] ; then
						if ! link "$DIR/$FILE" "$DIR$SUFFIX/$FILE" ; then
							rmdir "$LOCK"
							die "Failed to hard link '$DIR/$FILE'"
						fi
					fi
				done
			else
				warn "Missing '$DIR', cloning as an empty directory"
			fi
		done
		rmdir "$LOCK"
	fi
	log "Directories cloned"
}

function upload_blacklist
{
	DIR="$1"
	shift
	bin/send-indices $SEND_INDICES_OPTS --sendonly --force --local-index-name="$DIR$SUFFIX" --remote-index-name="$REMOTE_INDEX_NAME" --remote-suffix=$SUFFIX --files="$BLACKLIST parameters" --remote-homedir="$REMOTE_HOMEDIR" --keep-old "$@"
}

function swap_blacklist
{
	for SERVER in "$@" ; do
		log "Updating $REMOTE_INDEX_NAME on $SERVER"
		cmd "$SERVER" "bin/scontrol swap-blacklist $REMOTE_INDEX_NAME $BLACKLIST $SUFFIX" || {
			if [ $? = 2 ] ; then
				log "Invalid index version"
				exit 2
                        else
                                die "Updating of blacklists FAILED! Please, fix manually."
                        fi
                }
		sleep "$SWAP_DELAY" 
        done
}

function update_loop
{
echo $REMOTE_HOMEDIR
	if [ -z "$3" ] ; then
		usage
	fi
	INDICES="$1"
	DIR="$2"
	shift 2
	RETRY=$UPDATE_RETRY
	SHARED_FILES="parameters card-prints"
	while true ; do
		clone_dirs "$DIR"
		if [ ! -e "$DIR$SUFFIX/parameters" ] ; then
			warn "There is no index to update"
			return
		fi
		bin/black-gen -SIndexer.Blacklist=$BLACKLIST "$DIR$SUFFIX" $INDICES
		upload_blacklist "$DIR" "$@"
		(swap_blacklist "$@") || (
			[ $? != 2 ] && die "Update failed"
			[ $RETRY = 0 ] && die "Collision with another update, terminating"
			debug "Probably a collision with another update, waiting for retry"
			RETRY=$(($UPDATE_RETRY-1))
			sleep $UPDATE_DELAY
			continue
		)
		log "Blacklist updated successfully"
		return
	done
}

case "$1" in
	swap-dirs)		shift
				swap_dirs "$@"
				;;
	clone-dirs)		shift
				clone_dirs "$@"
				;;
	upload-blacklist)	shift
				upload_blacklist "$@"
				;;
	swap-blacklist)		shift
				swap_blacklist "$@"
				;;
	update-loop)		shift
				update_loop "$@"
				;;
	*)			usage
				;;
esac
