#!/bin/bash
# Make a hard-linked clone of a given state
# (c) 2004--2007 Martin Mares <mj@ucw.cz>

set -e
UCW_PROGNAME="shep-clone"
. lib/libucw.sh
if [ -n "$3" -o "$1" == "--help" ] ; then
	echo >&2 "Usage: shep-clone [<old> [<new>]]"
	exit 1
fi

new=
if [ -n "$1" ] ; then
	old=$1
else
	old=`cd db/state ; ls | sed '/^[0-9]\{8\}-[0-9]\{6\}$/!d' | tail -1`
	if [ -z "$old" ] ; then
		# Try old-style numbered states as used by shep-test
		old=`cd db/state ; ls | sed '/^[1-9][0-9]*$/!d' | sort -n | tail -1`
		if [ -z "$old" ] ; then
			bin/logger shep-clone '!' 'No state found'
			exit 1
		fi
		new=$(($old+1))
	fi
fi
if [ -n "$2" ] ; then
	new=$2
elif [ -z "$new" ] ; then
	new=`date '+%Y%m%d-%H%M%S'`
fi
olds=db/state/$old
news=db/state/$new
[ -d $olds ]  || die "State $old doesn't exist!"
[ ! -d $news ] || die "State $news already exists!"
log "Cloning state $olds to $news"
mkdir $news
ln $olds/index $news/
ln $olds/sites $news/
for a in contrib areas params control ; do
	[ ! -f $olds/$a ] || ln $olds/$a $news
done
rm -f db/state/current
ln -s $new db/state/current
