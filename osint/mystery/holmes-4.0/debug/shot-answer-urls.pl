#!/usr/bin/perl
#
#	Get URLs of answers to given queries
#	(c) 2007 Martin Mares <mj@ucw.cz>
#

use strict;
use warnings;
use Getopt::Long;

use lib 'lib/perl5';
use Sherlock::Query;

my $host = "localhost:8192";
my $max_processes = 3;

my $processes = 0;
my $cnt = 0;
my $pid;

sub wt() {
	$pid = wait;
	$pid >= 0 or die;
	print STDERR "Child $pid failed with exit code $?\n" if $?;
	$processes--;
}

while (<STDIN>) {
	chomp;
	my $query = $_;
	$cnt++;
	while ($processes >= $max_processes) {
		wt();
	}
	$pid = fork;
	$pid >= 0 or die;
	if ($pid) {
		$processes++;
	} else {
		my $q = Sherlock::Query->new("$host");
		my $stat = $q->query($query);
		if ($stat =~ /^[+]/) {
			my $report = "$cnt $query\n";
			foreach my $card (@{$q->{CARDS}}) {
				my $u = $card->get("(U") or die;
				my $url = $u->get("U") or die;
				$report .= "\t$url\n";
			}
			syswrite STDOUT, $report;
		} else {
			syswrite STDERR, "Query $cnt: $stat\n";
		}
		exit 0;
	}
}
while ($processes) {
	wt();
}
