#!/usr/bin/perl

use POSIX;

my $mode = 1;
if (defined $ARGV[0]) {
	if ($ARGV[0] eq "--absolute") {
		$mode = 0;
	} elsif ($ARGV[0] eq "--length") {
		$mode = 2;
	} else {
		die "Invalid mode $ARGV[0]";
	}
}

my $start;
while (<STDIN>) {
	chomp;
	my @a;
	if (@a = /^(.) (\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2}) (.*)/) {
		my $t = POSIX::mktime($a[6], $a[5], $a[4], $a[3], $a[2]-1, $a[1]-1900) or die "Invalid date: $_";
		my $r = $t;
		if (!defined($start)) { $start=$t; }
		if ($mode == 2) {
			$r = $t - $start;
			$start = $t;
		} elsif ($mode == 1) {
			$r = $t - $start;
		}
		print "$r\t$a[0] $a[7]\n";
	} else {
#		print "-\t$_\n";
	}
}
