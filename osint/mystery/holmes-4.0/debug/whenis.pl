#!/usr/bin/perl
# A simple decoder of timestamps
# Written by MM, not worth copyrighting.

use POSIX qw(strftime);
@ARGV == 1 or die "Usage: whenis <timestamp>";
print strftime("%a %d-%m-%Y %H:%M:%S %Z\n", localtime $ARGV[0]);
