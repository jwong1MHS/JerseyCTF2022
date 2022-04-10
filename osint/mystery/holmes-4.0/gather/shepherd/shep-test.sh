#!/bin/bash
# A simple testing script for Shepherd
# (c) 2003--2007 Martin Mares <mj@ucw.cz>

unset WAIT_FOR_KEY VERBOSE PRELOAD SINGLE_PHASE ALLOW_INIT LOGGING DELETE_UNUSED
CLEANUP_PERIOD=1000000000
INDEX_PERIOD=1000000000
DELETE_PERIOD=1
CYCLE_COUNTER=0
CLEANUP_COUNTER=0
while getopts "d:ei:Iklsvc:" opt ; do
	case "$opt" in
		c)	CLEANUP_PERIOD=$OPTARG
			;;
		d)	DELETE_PERIOD=$OPTARG
			DELETE_UNUSED=1
			;;
		e)	PRELOAD="LD_PRELOAD=/usr/lib/libefence.so"
			;;
		i)	INDEX_PERIOD=$OPTARG
			;;
		I)	ALLOW_INIT=1
			;;
		k)	WAIT_FOR_KEY=1
			;;
		l)	LOGGING=1
			;;
		s)	SINGLE_PHASE=1
			;;
		v)	VERBOSE=1
			;;
		*)	cat >&2 <<EOF

Usage: shep-test [<options>] <initial-state>

Options:
-c <period>	Run shep-cleanup every <period> cycles
-d <period>	Delete unused states except for every <period>-th cycle
-e		Run everything with Electric Fence preloaded
-i <period>	Run the indexer every <period> cycles
-I		Initialize a new database (don't give an <initial-state> then)
-l		Enable logging
-k		Wait for user input between modules
-s		Perform only a single phase
-v		Be verbose and dump contents of all files between modules

Files:
db/SHUTDOWN	Define if you want shep-test to stop after the current phase
EOF
			exit 1
			;;
	esac
done

function switch_logs()
{
	if [ -n "$LOGGING" ] ; then
		L=log/shepherd-`date '+%Y%m%d'`
		echo "Logging to $L"
		exec >>$L 2>&1
	fi
}

function run()
{
	echo "%%% Running $@"
	eval $PRELOAD "$@"
}

function key()
{
	if [ -n "$WAIT_FOR_KEY" ] ; then
		echo -n "... "
		read KEY
	fi
}

function dump()
{
	if [ -n "$VERBOSE" ] ; then
		bin/shep-dump "$@"
	fi
}

function newstate()
{
	echo "### Closing state $new ###"
	if [ -n "$old" -a -n "$DELETE_UNUSED" ] ; then
		if [ -z "$2" -o $(($CYCLE_COUNTER % $DELETE_PERIOD)) != 0 ] ; then
			if [ -f db/state/$old/LOCKED ] ; then
				echo "%%% Would like to delete state $old, but it's locked"
			else
				echo "%%% Deleting unused state $old"
				run rm -r db/state/$old
			fi
		fi
	fi

	switch_logs

	old=$new
	new=$(($old+1))
	echo "### Working on state $new ($1 phase) ###"
	echo "%%% Cloning state $old"
	olds=db/state/$old
	news=db/state/$new
	mkdir $news
	ln $olds/params $olds/index $olds/sites $news/
	[ ! -f $olds/areas ] || ln $olds/areas $news/
	[ ! -f $olds/contrib ] || ln $olds/contrib $news/
	rm -f db/state/current
	ln -s $new db/state/current
}

set -e

shift $(($OPTIND - 1))
if [ -n "$1" ] ; then
	new=$1
elif [ -z "$ALLOW_INIT" ] ; then
	echo "### Please run shep-test -I to initialize the database ###"
	exit 1
else
	switch_logs
	rm -rf db/state
	mkdir -p db/state
	>db/objects
	new=0
	news=db/state/0
	mkdir $news
	run bin/shep-init $news
	echo "### Created state 0 ###"
	run bin/shep $news --insert --urls - <db/start_urls
	dump -cu $news
	key
	run bin/shep-merge $news
	dump -iu $news
	key
	run bin/shep-record $news
	dump -iu $news
	key
	rm -f $news/contrib
fi
while true ; do
	CYCLE_COUNTER=$(($CYCLE_COUNTER+1))
	echo "### Starting cycle $CYCLE_COUNTER ###"
	newstate "reaping" keep
	run bin/shep-plan $news
	dump -pu $news
	key
	run bin/shep-reap $news
	dump -ju $news
	dump -c $news
	key
	run bin/shep-cork $news
	dump -s $news
	dump -iu $news
	key
	newstate "merging"
	run bin/shep-merge $news
	dump -s $news
	dump -iu $news
	key
	if [ $(($CYCLE_COUNTER % $INDEX_PERIOD)) == 0 ] ; then
		echo "### Calling indexer on state $new ###"
		run bin/shep-export $news $news/export
		key
		run bin/indexer -v -1 indexed:db/objects:$news/export
		key
		[ ! -f index/gatherer-feedback ] || mv index/gatherer-feedback db/feedback
		echo "### Indexing finished, continuing with merging on state $new ###"
	fi
	if [ -f db/feedback ] ; then
		run bin/shep-feedback $news db/feedback
		dump -iu $news
		key
		mv db/feedback db/feedback.old
	fi
	run bin/shep-equiv $news
	dump -s $news
	key
	run bin/shep-select $news
	dump -s $news
	dump -iu $news
	key
	run bin/shep-record $news
	dump -iu $news
	key
	run bin/shep-sort $news
	dump -iu $news
	key
	run bin/shep $news --histogram
	key
	rm -f $news/contrib
	CLEANUP_COUNTER=$(($CLEANUP_COUNTER+1))
	if [ $CLEANUP_COUNTER -ge $CLEANUP_PERIOD ] ; then
		if [ -f $news/LOCKED ] ; then
			echo "%%% Would like to run cleanup, but the state is locked"
		else
			newstate "cleanup"
			bin/shep-cleanup $news
			dump -iu $news
			key
			CLEANUP_COUNTER=0
		fi
	fi
	rm -f db/state/closed
	ln -sf $new db/state/closed
	sleep 1				# A phase must not take less than 1 sec
	if [ -n "$SINGLE_PHASE" -o -f db/SHUTDOWN ] ; then
		echo "### Shut down ###"
		rm -f db/SHUTDOWN
		exit 0
	fi
done
