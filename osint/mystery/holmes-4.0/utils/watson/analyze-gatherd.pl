#!/usr/bin/perl -w

# Precomputing data from v3.0 gatherer logs
#
# (c) 2003-2004 Tomas Valla <tom@ucw.cz>
#

use lib 'lib/perl5';
use Sherlock::Watsonlib;
use strict;
use warnings;

analyze_options() or die
"Reads Sherlock v3.0 gatherer logfile and generates interfile\n".analyze_usage();

chew_gatherdlog($ARGV[0],$ARGV[1]);

sub chew_gatherdlog {
	my ($infile,$outfile) = @_;

	# Refreshing not implemented yet
	my @keys = qw(
		total
		proc_time
		download
		redirect
		resolve
		err_soft
		err_hard
		refresh
		refr_ok
		refr_err_soft
		refr_err_hard
		not_modified
		not_changed
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
				$d{$k} = 0;
			}
			print $out "$s\n";
			$start_time = $tm - ($tm % $quantum);
			$lastpos=$in->tell;
		}

		if (my ($status,$reaper,$jobid,$refr_flag,$proc_time) = /: (\d{4}) .* \[(.*):([0-9a-f]*)(\*?)\] t=(\d+)/) {
			$d{'total'}++;
			$d{'proc_time'} += $proc_time;

			my $ms = int($status/1000);
			if ($ms == 0) {
				if ($status == 0 || $status==3 || $status==4) {$d{'download'}++}
				if ($status==1) {$d{'redirect'}++}
				elsif ($status==2) {$d{'resolve'}++}
				elsif ($status==3) {$d{'not_modified'}++}
				elsif ($status==4) {$d{'not_changed'}++}
			}
			elsif ($ms==1) {$d{'err_soft'}++}
			elsif ($ms==2) {$d{'err_hard'}++}
			if ($refr_flag eq "*") {
				$d{'refresh'}++;
				if ($ms == 0) {$d{'refr_ok'}++}
				elsif ($ms == 1) {$d{'refr_err_soft'}++}
				elsif ($ms == 2) {$d{'refr_err_hard'}++}
			} elsif ($refr_flag) { warning "Unknown refresh flag\n" }
		}
	}

	analyze_finish($in,$out,$infile,$lastpos);
}

