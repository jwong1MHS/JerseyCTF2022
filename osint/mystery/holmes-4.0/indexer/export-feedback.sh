#!/bin/bash
# Publish indexer's feedbacks
# (c) 2007 Pavel Charvat <pchar@ucw.cz>

set -e

if [ -z "$1" ] ; then
	echo "Usage: export-feedback <what> [<index>] [<dest>]" 1>&2
	exit 1
fi

INDEX="${2:-index}"

if [ "$1" = lex-stats ] ; then
	if [ -f "$INDEX/lexicon-by-freq" ] ; then
	        bin/idxdump -i "$INDEX" -Q | head -1002 >tmp/lex-stats
	        mkdir -p log/words
        	mv tmp/lex-stats log/words/lex-`date '+%Y%m%d'`
	        bin/logger export-feedback I "Published lexicon statistics."
	else
        	bin/logger export-feedback E "No lexicon statistics available for publishing."
	fi
	exit
fi

SRC="$INDEX/$1"
DEST="export/${3:-$1}"
if [ -f "$SRC" ] ; then
	if [ -f "$DEST" ] ; then
		rm -f "$DEST.old"
		ln "$DEST" "$DEST.old"
	fi
	mkdir -p export
	mv "$SRC" "$DEST"
	bin/logger export-feedback I "Published $DEST."
else
	bin/logger export-feedback E "No $DEST available for publishing."
fi
