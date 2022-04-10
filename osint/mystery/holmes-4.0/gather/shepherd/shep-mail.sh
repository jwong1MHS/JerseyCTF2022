#!/bin/bash
#
#  Sherlock Shepherd: Mail Report Script
#  (c) 2004 Martin Mares <mj@ucw.cz>
#

set -e
[ -z "$5" -a -n "$4" ]
ADDR="$1"
SUBJECT="$2"
LOGFILE="$3"
OFFSET="$4"
(
	cat <<EOF
Host:			`hostname -f`
Current directory:	`pwd`
Date and time:		`date`

EOF
	tail -c +$OFFSET $LOGFILE 2>&1
) | bin/send-mail -s "Shepherd: $2" $ADDR
