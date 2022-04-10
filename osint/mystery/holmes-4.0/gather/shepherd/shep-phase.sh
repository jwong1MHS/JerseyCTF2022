#!/bin/sh
# A trivial script for manipulating phases names in state control files
# (c) 2004 Martin Mares <mj@ucw.cz>

set -e
if [ "$1" == --help -o -z "$1" -o -n "$3" ] ; then
	echo >&2 "Usage: shep-phase <state> [<phase>]"
	exit 1
fi
STATE=$1
PHASE=$2
if [ ! -d $STATE ] ; then
	echo >&2 "No such state exists."
	exit 1
fi
if [ -z "$PHASE" ] ; then
	if [ -f $STATE/control ] ; then
		cat $STATE/control
	else
		echo "Control file missing."
	fi
else
	echo >$STATE/control $PHASE
	echo $PHASE
fi
