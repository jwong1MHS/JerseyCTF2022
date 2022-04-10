#!/usr/bin/perl -w

# (c) 2006 Pavel Charvat <pchar@ucw.cz>
#
# Reverse domain levels
#
# Typical usage:
#   cat urls | rev-domains.pl | sort | rev-domains.pl > sorted-urls

use strict;

while (<STDIN>) {
  if (/^(.*http:\/\/)([^\/:]*)([\/:].*)$/) {
    my $x = join(".", reverse split(/\./, $2));
    print "$1$x$3\n";
  }
  else {
    print;
  }
}
