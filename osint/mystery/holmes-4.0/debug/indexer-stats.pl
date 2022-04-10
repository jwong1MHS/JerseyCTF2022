#!/usr/bin/perl
# Analyze Indexer Timings

use strict;
use warnings;

use lib "lib/perl5";
use Sherlock::Watsonlib;

my %ignore = (
	"scheduler" => 0,
	"sizer" => 0,
	"indexer" => 1,
	"shcp" => 1,
	"send-indices" => 0
);

my $lmod = "";
my $lmt = 0;
my $total = 0;
my @mods = ();
my @times = ();

while (<>) {
	/^I (\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}) \[([^\]]+)\]/ or next;
	my $mod = $3;
	my $time = parse_timestamp($1, $2);
	$time > 0 or die "Bad timestamp";
	exists $ignore{$mod} && $ignore{$mod} > 0 && next;
	if ($mod ne $lmod) {
		if ($lmod ne "" && !exists $ignore{$lmod}) {
			push @mods, $lmod;
			push @times, $time-$lmt;
			$total += $time-$lmt;
		}
		$lmod = $mod;
		$lmt = $time;
	}
}

sub fmtime($) {
	my $t = shift;
	my $h = int ($t / 3600);
	$t %= 3600;
	return sprintf("%2d:%02d:%02d", $h, int($t/60), $t%60);
}

while (@mods) {
	my $mod = shift @mods;
	my $time = shift @times;
	printf "%-20s %-10s  %5.2f%%\n", $mod, fmtime($time), $time/$total*100;
}
printf "%-20s %-10s\n", "TOTAL", fmtime($total);
