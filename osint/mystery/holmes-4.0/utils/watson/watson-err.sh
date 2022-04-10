#!/bin/bash
# trivial Watson control script, add it into crontab

if [ -s log/watsonerr ]; then
	echo "Watson encountered some errors during periodical runs"
	echo "Look to Watson's log for more information"
	echo
	tail -n200 log/watsonerr
fi
