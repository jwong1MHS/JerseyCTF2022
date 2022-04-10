#!/usr/bin/perl -w

# Precomputing data from search server logs
#
# (c) 2003 Tomas Valla <tom@ucw.cz>
#

use lib 'lib/perl5';
use Sherlock::Watsonlib;
use strict;
use warnings;

analyze_options() or die
"Reads Sherlock searchserver logfile and generates interfile\n".analyze_usage();

chew_searchlog($ARGV[0],$ARGV[1]);

sub chew_searchlog {
	my ($infile, $outfile) = @_;
	my @keys = qw(
		total_qrs
		errors
		max_qr_time
		total_qr_time
		cache_hits
		);

	my $hdr_string = "#& start_time\t".join("\t",@keys)."\n";
	my ($in, $out, $fname, $seekpos) = analyze_init($infile,$outfile,$hdr_string);

	my $start_time;
	my %d = ();
	for my $k (@keys) {$d{$k}=0}
	$d{'max_qr_time'}=$d{'total_qr_time'}=undef;

	my $lastpos=$seekpos;
	my ($total_qrs, $errors, $cache_hits) = (0,0,0);
	my ($max_qr_time, $total_qr_time);

	while(<$in>) {
		my @line = split;
		my $cls = shift @line;
		my $date = shift @line;
		my $time = shift @line;
		my $pid = shift @line;
		my $x;
		if ($pid =~ /^\[[0-9]+\]$/) { $x = shift @line; }
		else { $x = $pid; }
		my $y = shift @line;
		my $t = shift @line;

		my $tm = parse_timestamp($date,$time);
		if ($tm==-1) {
			warning("Bad timestamp");
			next;
		}

		defined $start_time or $start_time = $tm- ($tm % $quantum);

		if ($tm>$start_time+$quantum) {
			my $s = "$start_time";
			for my $k (@keys) {
				$s.="\t".(defined $d{$k} ? $d{$k} : "x");
			}
			print $out "$s\n";

			for my $k (@keys) {$d{$k}=0}
			$d{'max_qr_time'}=$d{'total_qr_time'}=undef;
			$lastpos = $in->tell;
			$start_time = $tm- ($tm % $quantum);
		}

		next unless $cls eq "I";
		# we don't need any info from "<" lines, maybe later...
		if ($x eq ">") {
			$d{'total_qrs'}++;
			$d{'errors'}++ if $y!=0;
			my ($tm) = $t=~/t=(.*)/;
			defined $d{'max_qr_time'} or $d{'max_qr_time'}=0;
			$d{'max_qr_time'}=$tm if $tm>$d{'max_qr_time'};
			defined $d{'total_qr_time'} or $d{'total_qr_time'}=0;
			$d{'total_qr_time'} += $tm;
			if (/\scage:\d+\s/) {$d{'cache_hits'}++}
		}
	}

	analyze_finish($in,$out,$infile,$lastpos);
}

