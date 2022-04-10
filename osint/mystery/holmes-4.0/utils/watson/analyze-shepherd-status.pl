#!/usr/bin/perl -w

# Precomputing status data from shepherd logs
#
# (c) 2005 Vladimir Jelen <vladimir.jelen@netcentrum.cz>

use lib 'lib/perl5';
use Sherlock::Watsonlib;
use strict;

analyze_options() or die
"Reads Sherlock shepherd logfile and generates status interfile\n".analyze_usage();

chew_shepherdlog($ARGV[0], $ARGV[1]);

sub chew_shepherdlog {
	my ($infile, $outfile) = @_;

	my @keys = qw(
		gather
		plan
		cleanup
		down
		error);

	my $hdr_string = "#& start_time\t".join("\t",@keys)."\n";

	my ($in, $out, $fname, $seekpos) = analyze_init($infile, $outfile, $hdr_string);

	my $start_time;
	my $lastpos = $seekpos;
	my %d = ();
	for my $k (@keys) { $d{$k} = 0; }

	local *write_inter_file = sub {
		my $change = 0;
		for my $k (@keys) { if($d{$k}) { $change = 1; last; } }

		if($change) {
			my $s = "$start_time";
			for my $k (@keys) {
				$s .= "\t" . $d{$k};
			}
			print $out "$s\n";
		}
		$lastpos = $in->tell;
	};

	while(<$in>) {
		/^(.) (\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}) \[([^\]]+)\] (.+)$/ or next;

		my $time = parse_timestamp($2, $3);
		if ($time == -1) {
			warning("Bad timestamp");
			next;
		}

		defined $start_time or $start_time = $time - ($time % $quantum);

		if($time > $start_time + $quantum) {
			write_inter_file();
			$start_time = $time - ($time % $quantum);
		}

		$_ = $4;
		my $status = $1;
		my $text = $5;
		SWITCH: {
			/^shep-reap$/ && do {
				for my $k (@keys) { $d{$k} = 0; }
				$d{'gather'} = 1;
				last SWITCH;
			};
			/^shep-cork|shep-merge|shep-feedback|shep-equiv|shep-select|shep-record|shep-plan|urltool$/ && do {
				for my $k (@keys) { $d{$k} = 0; }
				$d{'plan'} = 1;
				last SWITCH;
			};
			/^shep-cleanup$/ && do {
				for my $k (@keys) { $d{$k} = 0; }
				$d{'cleanup'} = 1;
				last SWITCH;
			};
			/^shepherd$/ && $text =~ /^Shepherd shut down\.$/ && do {
				for my $k (@keys) { $d{$k} = 0; }
				$d{'down'} = 1;
				last SWITCH;
			};
			/^shepherd$/ && $status =~ /^!$/ && do {
				for my $k (@keys) { $d{$k} = 0; }
				$d{'error'} = 1;
				last SWITCH;
			};
		}
	}
	write_inter_file();
	analyze_finish($in, $out, $infile, $lastpos);
}
