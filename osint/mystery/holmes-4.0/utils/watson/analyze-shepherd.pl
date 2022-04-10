#!/usr/bin/perl -w

# Precomputing data from shepherd logs
#
# (c) 2004 Tomas Valla <tom@ucw.cz>
# (c) 2006 Pavel Charvat <pchar@ucw.cz>
#

use lib 'lib/perl5';
use Sherlock::Watsonlib;
use strict;
use warnings;

analyze_options() or die
"Reads Sherlock shepherd logfile and generates interfile\n".analyze_usage();

chew_schedulerlog($ARGV[0],$ARGV[1]);

sub chew_schedulerlog {
	my ($infile,$outfile) = @_;

	my @keys = qw(
		jobs_sec
		jobs_avg_active
		jobs_avg_prefetched
		jobs_avg_ready
		jobs_remain

		reap_gathered
		reap_planned

		contribs_found
		contribs_new
		contribs_recorded

		bucket_size
		space_avail
		space_estim

		url_new
		url_gathered
		url_active
		url_inactive
		url_zombie

		act_woken_up
		act_lulled
		act_lost_gathered
		act_discarded

		perf_limit
		perf_limit_fr
		perf_utilized
		perf_utilized_fr

		size_active
		size_active_estim
		size_inactive
		size_inactive_estim

		site_alive
		site_unres
		);
		# jobs_sec_reaper_*
		# gathered_section_*
		# bucket_size is in MB

	my $hdr_string="#@\n";

	my ($in, $out, $fname, $seekpos)=analyze_init($infile,$outfile,$hdr_string);

	my $start_time;
	my $lastpos=$seekpos;
	my $currpos=$seekpos;
	my %d=();
	my $reaping_time = 0;
	my $inside_inactive = 0;
	my $inside_discarded = 0;

	while (<$in>) {
		/^I/ or next;
		/^. (\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}) / or next;
		my $time=parse_timestamp($1,$2);
		if ($time==-1) {
			warning("Bad timestamp");
			next;
		}

		defined $start_time or $start_time = $time - ($time % $quantum);

		if ($time>$start_time+$quantum) {
			for my $k (keys %d) {
				if ($k =~ /^jobs_sec/) {
					$d{$k} = $reaping_time ? $d{$k} / $reaping_time : 0;
				}
			}
			for my $k (sort keys %d) {
				defined $d{$k} and print $out "$start_time\t$k\t$d{$k}\n";
			}

			$reaping_time = 0;
			$start_time = $time - ($time % $quantum);
			$lastpos = $currpos;
			%d = ();
		}

		if (/Bucket file size: (\d+) MB/) {
			$d{'bucket_size'} = $1;
		}
		elsif (/\[shep-select\]/) {
			if (/Output: (\d+) URL's \((\d+) active \[(\d+) contrib \+ (\d+) new \+ (\d+) gathered\], (\d+) inactive \[(\d+) sleeping \+ (\d+) zombies\]\)/) {
				$d{'url_active'} = $2;
				$d{'url_new'} = $3 + $4;
				$d{'url_gathered'} = $5;
				$d{'url_inactive'} = $6;
				$d{'url_zombie'} = $8;
			}
			elsif (/Lost (\d+) gathered URL's/) {
				$d{'act_lost_gathered'} = $1;
			}
			elsif (/Inactive:/) {
				$inside_inactive = 1;
			}
			elsif (/Discarded:/) {
				$inside_discarded = 1;
			}
			elsif ($inside_inactive && /(New|Contrib|Gathered):.*\s+(\d+)\s*$/) {
				$d{'act_lulled'} = $2;
			}
			elsif (/Total:.*\s+(\d+)\s*$/) {
				if ($inside_discarded) {
					$d{'act_discarded'} = $1;
					$inside_discarded = 0;
				}
				elsif ($inside_inactive) {
					$inside_inactive = 0;
				}
			}
			elsif (/Active: \d+ gathered, \d+ new \[\d+ inherited \+ (\d+) woken up\]/) {
				$d{'act_woken_up'} = $1;
			}
			elsif (/Performance utilized: (\d+) \((\d+) for frequent refresh\)/) {
				$d{'perf_utilized'} = $1;
				$d{'perf_utilized_fr'} = $2;
			}
			elsif (/Performance limit: (\d+) soft, (\d+) hard, (\d+) frequent refresh/) {
				$d{'perf_limit'} = $1;
				$d{'perf_limit_fr'} = $3;
			}
			elsif (/Space: (\d+)M available/) {
				$d{'space_avail'} = $1;
			}
			elsif (/Space estimated: (\d+)M/) {
				$d{'space_estim'} = $1;
			}
			elsif (/Section (\d+): (\d+) gathered/) {
				$d{'gathered_section_'.$1} = $2;
			}
			elsif (/Sites: (\d+) alive, (\d+) unres, \d+ dead, \d+ pruned; \d+ died, \d+ revived/) {
				$d{'site_alive'} = $1;
				$d{'site_unres'} = $2;
			}
			### Compatibility with older output formats ###
			elsif (/Output: (\d+) URL's \((\d+) active \[(\d+) new \+ (\d+) gathered\], (\d+) inactive\)/) {
				$d{'url_active'} = $2;
				$d{'url_new'} = $3;
				$d{'url_gathered'} = $4;
				$d{'url_inactive'} = $5;
			}
		}
		elsif (/\[shep-merge\]/) {
			/Found (\d+) contributions/ and $d{'contribs_found'} = $1;
			/Done: (\d+) sacred entries, (\d+) new contribs/ and $d{'contribs_new'} = $2;

		}
		elsif (/\[shep-record\]/) {
			/Created (\d+) buckets/ and $d{'contribs_recorded'} = $1;

		}
		elsif (/\[shep-cleanup\]/) {
			if (/Average sizes active, inactive: (\d+), (\d+) \(predicted (\d+), (\d+)\)/ ||
			    /Average sizes thick, thin: (\d+), (\d+) \(predicted (\d+), (\d+)\)/) {
				$d{'size_active'} = $1;
				$d{'size_inactive'} = $2;
				$d{'size_active_estim'} = $3;
				$d{'size_inactive_estim'} = $4;
			}
		}
		elsif (/\[shep-reap\]/) {
			if (/Reaper statistics for last (\d+) seconds:/) {
				$reaping_time += $1;
			}
			if (/Gathered (\d+) of (\d+) planned URLs/) {
				$d{'reap_gathered'} = $1;
				$d{'reap_planned'} = $2;
			} elsif (/ (\w+): *(\d+) jobs ok/) {
				if ($1 eq "Total") {
					(defined $d{'jobs_sec'}) ? ($d{'jobs_sec'} += $2) : ($d{'jobs_sec'} = $2);
				} elsif (/ Plan:\s+([0-9.]+)\/([0-9.]+)\/([0-9.]+) avg active\/prefetched\/ready, (\d+) keys remain/) {
					$d{'jobs_avg_active'} = $1;
					$d{'jobs_avg_prefetched'} = $2;
					$d{'jobs_avg_ready'} = $3;
					$d{'jobs_remain'} = $4;
				} else {
					(defined $d{'jobs_sec_reaper_'.$1}) ? ($d{'jobs_sec_reaper_'.$1} += $2) : ($d{'jobs_sec_reaper_'.$1} = $2);
				}
			}
		}
	$currpos = $in->tell;
	}
	for my $k (keys %d) {
		if ($k =~ /^jobs_sec/) {
			$d{$k} = $reaping_time ? $d{$k} / $reaping_time : 0;
		}
	}
	for my $k (sort keys %d) {
		defined $d{$k} and print $out "$start_time\t$k\t$d{$k}\n";
	}

	analyze_finish($in,$out,$infile,$lastpos);
}

