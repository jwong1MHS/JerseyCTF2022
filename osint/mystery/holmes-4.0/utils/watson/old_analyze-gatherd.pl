#!/usr/bin/perl -w

# Precomputing data from gatherd logs
#
# (c) 2003 Tomas Valla <tom@ucw.cz>
#

use lib 'lib/perl5';
use Sherlock::Watsonlib;
use strict;
use warnings;

analyze_options() or die
"Reads Sherlock gatherer logfile and generates interfile\n".analyze_usage();

chew_gatherdlog($ARGV[0],$ARGV[1]);

sub chew_gatherdlog {
	my ($infile,$outfile) = @_;

	my @keys = qw(
		total
		download
		redirect
		resolve
		err_soft
		err_hard
		duplicate
		refresh
		refr_ok
		refr_err_soft
		refr_err_hard
		added
		delay
		queue_prior_sum
		);
	my $hdr_string="#& start_time\t".join("\t",@keys)."\n";

	my ($in, $out, $fname, $seekpos)=analyze_init($infile,$outfile,$hdr_string);

	my $lastpos=$seekpos;
	my %d = ();
	for my $k (@keys) {$d{$k}=0}
	my $start_time;

	while(<$in>) {
		my ($cls,$date,$time,$url,$rest)=split;
		next unless $cls eq "I" ;
		my $tm = parse_timestamp($date,$time);
		if ($tm==-1) {
			warning("Bad timestamp");
			next;
		}

		defined $start_time or $start_time = $tm - ($tm % $quantum);

		if ($tm>$start_time+$quantum) {
			my $s="$start_time";
			for my $k (@keys) {
				$s.="\t".$d{$k};
				$d{$k}=0;
			}
			print $out "$s\n";
			$start_time = $tm - ($tm % $quantum);
			$lastpos=$in->tell;
		}

		if (my ($status,$pid,$refr,$flag) = /: (\d{4}) .* \[(\d*)(\*?)(.?)\]/) {
			$d{'total'}++;
			my $dly;
			$dly = 0 unless ($dly) = ($rest =~ /\] d=(\d+)/);
			$d{'delay'}+=$dly;
			next unless $status=~/^\d+$/;
			my $ms = int($status/1000);
			if ($ms == 0) {
				if ($status == 0 || $status==3 || $status==4) {$d{'download'}++}
				elsif ($status==1) {$d{'redirect'}++}
				elsif ($status==2) {$d{'resolve'}++}
				else {die "Unknown status code $status\n"}
			}
			elsif ($ms==1) {$d{'err_soft'}++}
			elsif ($ms==2) {$d{'err_hard'}++}
			else {die "Unknown status code $status\n"}
			if ($refr) {
				$d{'refresh'}++;
				if ($ms==0 && $flag ne "") {$d{'refr_ok'}++}
				elsif ($ms==1) {$d{'refr_err_soft'}++}
				elsif ($ms==2) {$d{'refr_err_hard'}++}
			}
			if (defined $flag && ($flag eq "+" || $flag eq "!")) {
				if ($ms==0) {$d{'added'}++}
				if ($refr) {$d{'added'}--}
				if ($flag eq "!") {$d{'duplicate'}++}
			}
			if (/\sp=(\d+)\s*$/) {
				$d{'queue_prior_sum'}+=$1;
			}
		}
	}

	analyze_finish($in,$out,$infile,$lastpos);
}

