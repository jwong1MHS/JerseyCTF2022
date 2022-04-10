#!/usr/bin/perl
# Calculate timing statistics from log/sherlockd (run after log-qsplit if using Hydra mode)
# (c) 2001--2005 Martin Mares <mj@ucw.cz>

use strict;
use warnings;
use Getopt::Long;

my $show_errors = 0;
my $top_n = 5;

GetOptions(
	   "top=i" => \$top_n
) or die <<EOF ;
Usage: $0 <options> [<logfiles>]

Options:
--top=<n>	Show <n> top queries for each parameter
EOF

my $cnt=0;
my @hourly = map { 0 } 1..24;
my $current_date = '';
my $current_hour = 0;
my $current_total = 0;
my @queries = ();
my @timing = ();
my %profs = ();
my %cnts = ();
print "Parsing log and dumping errors:\n";
while (<>) {
	/^[^ID] / && print;
	my ($date,$hour,$addr,$msg) = m/^. (\d{4}-\d{2}-\d{2} (\d{2})):\d{2}:\d{2} (\[\d+] )?(.*)\n/ or die "Syntax error: $_";
	$_ = $msg;
	if (/^[0-9.]+ < (.*)/) {
		$queries[++$cnt] = $1;
		$timing[$cnt] = {};
		if ($current_date ne $date) {
			print "# $current_date $current_total\n" if $current_date;
			$current_date = $date;
			$current_hour = $hour;
			$current_total = 0;
		}
		if (++$current_total > $hourly[$current_hour]) { $hourly[$current_hour] = $current_total; }
	} elsif (/^> \d+ (.*)/) {
		my $t = $1;
		my $h = $timing[$cnt];
		$t =~ s/(\w+)[=]([0-9.]+)/$$h{$1}=$2; $profs{$1}=1; ""/ge;
		$t =~ s/(\w+)[:]([0-9.]+)/$$h{$1}=$2; $cnts{$1}=1; ""/ge;
	}
}
print "# $current_date $current_total\n" if $current_date;
print "Seen $cnt queries.\n";
$cnt || exit 0;

print "\n";
print "Hourly statistics:\n";
print "       ", join(" ", map { sprintf("%5d", $_) } 0..23), "\n";
print "Peaks: ", join(" ", map { sprintf("%5d", $_) } @hourly), "\n";
print "QPS:   ", join(" ", map { sprintf("%5.1f", $_/3600) } @hourly), "\n";
print "SPQ:   ", join(" ", map { $_ ? sprintf(($_ < 36 ? "%5.0f" : "%5.1f"), 3600/$_) : "-----" } @hourly), "\n";
print "\n";

delete $profs{'t'};
my @timers = sort keys %profs;
unshift @timers, "t";
push @timers, sort keys %cnts;

my %card = map { $_ => 0 } @timers;
my %total = map { $_ => 0 } @timers;
my %peak = map { $_ => 0 } @timers;
for (my $i=0; $i<$cnt; $i++) {
	my $t = $timing[$i];
	foreach my $p (keys %$t) {
		$card{$p}++;
		$total{$p} += $$t{$p};
		($$t{$p} > $peak{$p}) && ($peak{$p} = $$t{$p});
	}
}

print "       ", join(" ", map { sprintf("%10s", $_) } @timers), "\n";
print "Card:  ", join(" ", map { sprintf("%10d", $card{$_}) } @timers), "\n";
print "Total: ", join(" ", map { sprintf("%10.5g", $total{$_}) } @timers), "\n";
print "Peak:  ", join(" ", map { sprintf("%10.5g", $peak{$_}) } @timers), "\n";
print "Avg:   ", join(" ", map { ($card{$_} > 0) ? sprintf("%10.5g", $total{$_}/$card{$_}) : "---------" } @timers), "\n";

if ($top_n) {
	print "\n";
	foreach my $p (@timers) {
		if ($p eq "t") { print "Query times:\n"; }
		elsif (defined $profs{$p}) { print "prof_$p:\n"; }
		else { print "stat_$p:\n"; }
		print "\tCardinality: $card{$p}\n";
		if ($card{$p}) {
			printf "\tAverage: %.3f\n", $total{$p} / $card{$p};
			print "\tPeak: $peak{$p}\n";
			my @rec = sort {
				if (!defined $timing[$b]{$p}) { -1; }
				elsif (!defined $timing[$a]{$p}) { 1; }
				else { $timing[$b]{$p} <=> $timing[$a]{$p} }
			} 0..$cnt-1;
			for (my $i=0; $i<$top_n && $i<$card{$p}; $i++) {
				my $j = $rec[$i];
				print "\t[$i] ", $timing[$j]{$p}, " ", $queries[$j], "\n";
				my $tt = "";
				foreach my $q (@timers) {
					$tt .= " $q=" . $timing[$j]{$q} if (defined $timing[$j]{$q})
				}
				$tt =~ s/^\s+//;
				print "\t\t$tt\n";
			}
		}
	}
}
