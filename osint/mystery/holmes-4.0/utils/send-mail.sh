#!/bin/bash
#
#  Mail Notification Script.
#  (c) 2006 Pavel Charvat <pchar@ucw.cz>
#

unset SUBJ
if [ "$1" = -s ] ; then
  SUBJ="$2"
  shift 2
fi

if [ -n "$1" ] ; then
	mutt -x -e 'set charset="utf-8"; set send_charset="us-ascii:iso-8859-2:utf-8"; '"my_hdr From: \"Sherlock at `hostname`\" <`whoami`>;" -s "$SUBJ" $@
fi

