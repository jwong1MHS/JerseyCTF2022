#!/bin/bash
# Script for automatic recovery after gatherer crash.
# (c) 2002--2003 Martin Mares <mj@ucw.cz>

set -e

log()
{
	bin/logger grecover "$@"
}

# First of all, detect what state we were stuck in.
# Existing states:
#   gathering
#   expiring
#   shakedown
#   unknown_state
#   recovering
#   reconstructing

if [ -s db/GATHERLOCK ] ; then
	STATE=`cat db/GATHERLOCK`
elif [ -f db/GATHERLOCK ] ; then
	STATE=unknown_state
elif [ -f db/RECOVERING ] ; then
	STATE=recovering
else
	exit 0
fi
rm -f db/GATHERLOCK
>db/RECOVERING

log W "Recovering databases after crash in the middle of $STATE..."

# Recovery after incomplete shakedown
if [ $STATE = shakedown -o $STATE = reconstructing ] ; then
	log W "Huh, it was during shakedown, so fscking bucket files and let's keep the fingers crossed."
	bin/buckettool -F
	log I "Bucket file fixed, now synchronizing URLdb."
	bin/gc -X
	log I "URLdb synchronized, but robots.txt lost, so expect lots of unreferenced buckets."
fi

# Check and repair the bucket file
if bin/buckettool -q ; then
	log I "Bucket file looks consistent."
else
	log W "Bucket file is inconsistent, checking and fixing it."
	bin/buckettool -F
	log I "Bucket file errors fixed."
fi

# Use the expirer to identify missing buckets, fix minor database
# problems and reconstruct queue.
log I "Recovering databases and queues."
bin/expire -f

rm db/RECOVERING
log I "Gatherer hopefully recovered."
