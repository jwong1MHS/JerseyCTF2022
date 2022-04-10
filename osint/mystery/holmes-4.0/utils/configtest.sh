#!/bin/sh
# A simple utility for checking of configuration and filters
# (c) 2005 Martin Mares <mj@ucw.cz>

set -e
echo "Enter URL's for testing:"
if [ -f cf/test-filter ] ; then
	F=cf/test-filter
else
	F=cf/filter
fi
bin/filter-test $F --all-vars --verdicts --urlkey --dump
