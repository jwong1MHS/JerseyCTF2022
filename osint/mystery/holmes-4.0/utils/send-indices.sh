#!/bin/bash
#
#  Upload indices to multiple search servers
#  (c) 2004 Martin Mares <mj@ucw.cz>
#  (c) 2004 Robert Spalek <robert@ucw.cz>
#  (c) 2005-2006 Vladimir Jelen <vladimir.jelen@netcentrum.cz>
#  (c) 2006 Pavel Charvat <pchar@ucw.cz>
#

set -e

IN_SSH_AGENT=
SSH_OPTIONS=
SHCP_OPTIONS=
if [ "$1" == "--have-agent" ] ; then
	IN_SSH_AGENT=1
	SSH_OPTIONS="-A"
	SHCP_OPTIONS="-a -A"
	shift
fi

function usage
{
	cat >&2 <<EOF
Usage: send-indices <options> <search-server...>

Options:
--sendonly		Only upload the index, don't make it active
--swaponly		Do not upload anything, only swap index/indices
--force			Force index upload even if versions seem to match
--remote-homedir=...	Use ... as remote homedir instead of default "run"
--local-index-name=...	Use ... as local index name instead of default "index"
--remote-index-name=...	Use ... as remote index name instead of default "index"
--remote-suffix=...	Use ... as upload directory suffix instead of default ".new"
--direct		Use direct IO (uncached)
--limit=...		Transfer speed limit (in MB/s)
--swap-delay=...	Time delay between index swap (in sec)
--agent			Start ssh-agent and use it for remote authentication
--key=...		Add a custom key to the ssh-agent (default=~/.ssh/send-index-key)
--append		Append files to partially uploaded index
--files=...		Index files to transfer (default=standard index files)
--extra-files=...	Additional files to transfer
--keep-old		Do not delete old index

Note: * When uploading is requested, you can specify only one index to transfer
      * When \`--swaponly' is requested, you can specify more indices to swap at once
        with \`--remote-index-name="index1 index2 index3 ..."'
EOF
	exit 1
}

SENDONLY=
SWAPONLY=
FORCE=
REMOTE_HOMEDIR=run
LOCAL_INDEX_NAME=index
REMOTE_INDEX_NAME=index
REMOTE_SUFFIX=.new
DIRECT=
LIMIT=
SWAP_DELAY=120
APPEND=
INDEX_FILES="cards card-attrs card-prints sites references lexicon stems string-map string-hash parameters"
EXTRA_FILES=
KEEP_OLD=
KEY=~/.ssh/send-index-key

while [ "${1:0:2}" == "--" ] ; do
	case "$1" in
		--sendonly)		SENDONLY=1
					;;
		--swaponly)		SWAPONLY=1
					;;
		--force)		FORCE=1
					;;
		--remote-homedir=*)	REMOTE_HOMEDIR=${1:17}
					;;
		--local-index-name=*)	LOCAL_INDEX_NAME=${1:19}
					;;
		--remote-index-name=*)	REMOTE_INDEX_NAME=${1:20}
					;;
		--remote-suffix=*)	REMOTE_SUFFIX=${1:16}
					;;
		--direct)		DIRECT='-d'
					;;
		--limit=*)		LIMIT="-l ${1:8}"
					;;
		--swap-delay=*)		SWAP_DELAY=${1:13}
					;;
		--agent)		[ -z "$IN_SSH_AGENT" ] && exec ssh-agent "$0" --have-agent "$@"
					;;
		--key=*)		KEY="${1:6}"
					;;
		--append)		APPEND=1
					;;
		--files=*)		INDEX_FILES="${1:8}"
					;;
		--extra-files=*)	EXTRA_FILES="${1:14}"
					;;
		--keep-old)		KEEP_OLD=1
					;;
		*)			usage
					;;
	esac
	shift
done
[ -n "$1" ] || usage

if [ -n "$IN_SSH_AGENT" ] ; then
	ssh-add "$KEY"
fi

INDEX_FILES="$INDEX_FILES $EXTRA_FILES"

function log
{
	bin/logger send-indices I "$1"
}

function debug
{
	bin/logger send-indices D "$1"
}

function die
{
	bin/logger send-indices ! "$1"
	exit 1
}

function cmd
{
	debug "Sending to $1: set -e ; cd $REMOTE_HOMEDIR ; $2"
	ssh $SSH_OPTIONS $1 "set -e ; cd $REMOTE_HOMEDIR ; $2"
}

function send-indicies
{
	SOURCE=""
	DESTINATION=""
	DEST_APPEND=""
	SERVERS=""
	for FILE in $INDEX_FILES ; do
		[ -f "$LOCAL_INDEX_NAME/$FILE" ] && SOURCE="$SOURCE $LOCAL_INDEX_NAME/$FILE"
	done
	[ -n "$SOURCE" ] || die "No files to transfer!"
	LOCAL_VERSION=`bin/index-version $LOCAL_INDEX_NAME $INDEX_FILES || die 'Malformed local index'`
	if [ -n "$FORCE" ] ; then
		LOCAL_VERSION="force-new"
	fi

	for SERVER in "$@" ; do
		REMOTE_VERSION=`ssh $SSH_OPTIONS $SERVER "cd $REMOTE_HOMEDIR && bin/index-version $REMOTE_INDEX_NAME$REMOTE_SUFFIX $INDEX_FILES || true"`
		[ -n "$REMOTE_VERSION" ] || die "Cannot get version of $REMOTE_INDEX_NAME$REMOTE_SUFFIX"
		debug "Current version of $REMOTE_INDEX_NAME$REMOTE_SUFFIX: $REMOTE_VERSION"
		if [ ${REMOTE_VERSION:0:1} != "<" ] ; then
			if [ $REMOTE_VERSION == $LOCAL_VERSION ] ; then
				log "$REMOTE_INDEX_NAME$REMOTE_SUFFIX on $SERVER already has version $LOCAL_VERSION."
				continue
			else
				log "$REMOTE_INDEX_NAME$REMOTE_SUFFIX on $SERVER has a different version $REMOTE_VERSION, replacing it."
			fi
		fi
		SERVERS="$SERVERS$SERVER "
		if [ -z "$APPEND" ] ; then
			OLD=
			if [ -z "$KEEP_OLD" ] ; then
			  	OLD="$REMOTE_INDEX_NAME.old"
			fi
			cmd "$SERVER" "rm -rf $REMOTE_INDEX_NAME$REMOTE_SUFFIX $OLD ; mkdir -p $REMOTE_INDEX_NAME$REMOTE_SUFFIX"
		fi
	done

	shift
	for SERVER in $SERVERS ; do
		if [ -z "$DESTINATION" ] ; then
			DESTINATION="$SERVER:$REMOTE_INDEX_NAME$REMOTE_SUFFIX"
		else
			DESTINATION="$DESTINATION { $SERVER:$REMOTE_INDEX_NAME$REMOTE_SUFFIX"
			DEST_APPEND="$DEST_APPEND }"
		fi
	done
	DESTINATION="$DESTINATION$DEST_APPEND"

	if [ -n "$SERVERS" ] ; then
		debug "Executing cmd: bin/shcp $SHCP_OPTIONS $DIRECT $LIMIT -e $REMOTE_HOMEDIR { $SOURCE } $DESTINATION"
		bin/shcp $SHCP_OPTIONS $DIRECT $LIMIT -e $REMOTE_HOMEDIR { $SOURCE } $DESTINATION || die "Index transfer failed"
	fi

	for SERVER in $SERVERS ; do
		TEST_VERSION=`ssh $SSH_OPTIONS $SERVER "cd $REMOTE_HOMEDIR && bin/index-version $REMOTE_INDEX_NAME$REMOTE_SUFFIX $INDEX_FILES || true"`;
		[ -n "$TEST_VERSION" ] || die "Cannot get version of $REMOTE_INDEX_NAME$REMOTE_SUFFIX"
		log "Index version $TEST_VERSION received on $SERVER"
		if [ $TEST_VERSION != $LOCAL_VERSION -a $LOCAL_VERSION != force-new ] ; then
			die "Version mismatch: expected $LOCAL_VERSION, received $TEST_VERSION on $SERVER"
		fi
	done
}

if [ -z "$SWAPONLY" ] ; then
	log 'Uploading indices...'
	send-indicies "$@"
	log 'All indices uploaded.'
fi
if [ -z "$SENDONLY" ] ; then
	log "Switching indices \"$REMOTE_INDEX_NAME\"..."
	for SERVER in "$@" ; do
		log "Swapping $REMOTE_INDEX_NAME on $SERVER"
		ssh $SSH_OPTIONS $SERVER "cd $REMOTE_HOMEDIR && NEW_NAME=\`echo $REMOTE_INDEX_NAME\`\" \" && bin/scontrol swap \${NEW_NAME//.new / }" || die "Swapping of indices FAILED! Please fix manually."
		sleep "$SWAP_DELAY"
	done
	log "All indices switched."
fi
