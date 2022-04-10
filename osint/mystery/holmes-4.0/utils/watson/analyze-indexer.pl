#!/usr/bin/perl -w

# Precomputing data from indexer logs
#
# (c) 2003-2005 Tomas Valla <tom@ucw.cz>
# (c) 2006 Vladimir Jelen <vladimir.jelen@netcentrum.cz>
# (c) 2007 Pavel Charvat <pchar@ucw.cz>
#

use lib 'lib/perl5';
use Sherlock::Watsonlib;
use strict;
use warnings;

analyze_options() or die
"Reads Sherlock indexer logfile and generates interfile\n".analyze_usage();

# Recompute whole interfile, because we don't remember which index we are processing
$force = 1;

my $last_index = '';
my $sending = 0;

chew_indexerlog($ARGV[0],$ARGV[1]);

sub chew_indexerlog {
	my ($infile,$outfile) = @_;

	my @keys = qw(
		ft_in_unknown
		ft_in_html
		ft_in_pdf
		ft_in_text
		ft_in_msword
		ft_in_excel
		ft_in_jpeg
		ft_in_png
		ft_in_gif
		ft_out_unknown
		ft_out_html
		ft_out_pdf
		ft_out_text
		ft_out_msword
		ft_out_excel
		ft_out_jpeg
		ft_out_png
		ft_out_gif
		lang_unknown
		lang_en
		lang_cs
		lang_sk
		lang_pl
		lang_hu
		lang_de
		lang_nl
		lang_fr
		lang_es
		lang_it
		lang_ru
		indexing_start
		indexing_end
		idxcopy_start
		idxcopy_end
		scan_obj
		scan_doc
		scan_card
		chewer_card
		merge_card
		merge_dupl
		merge_classes
		merge_penal
		mklex_words
		size_md5
		size_url
		size_hosts
		size_objs
		size_queue
		size_attrs
		size_cards
		size_lexicon
		size_refs
		size_string
		size_total );
		# chewer_card_*
		# size_total_*
		# size_lexicon_*

	my $hdr_string="#@\n";

	my ($in, $out, $fname, $seekpos)=analyze_init($infile,$outfile,$hdr_string);

	my $start_time;
	my $lastpos=$seekpos;
	my %d=();

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
			for my $k (sort keys %d) {
				defined $d{$k} and print $out "$start_time\t$k\t$d{$k}\n";
			}

			$start_time = $time - ($time % $quantum);
			$lastpos=$in->tell;
			%d=();
		}

		if (/\[indexer] Processing subindex (\S+)/) {
			$last_index = $1;
		}
		elsif (/\[indexer] Building index/) {
			$d{'indexing_start'} = $time;
		}
		elsif (/\[indexer] Index built successfully/) {
			$d{'indexing_end'} = $time;
		}
		elsif (/\[scanner] Scanned (\d+) objects, created (\d+) cards/)	{ # old-style
			$d{'scan_obj'} = $1;
			$d{'scan_card'} = $2;
		}
		elsif (/\[scanner] Scanned (\d+) objects \((\d+) ok, (\d+) err, (\d+) robots, (\d+) (new|skeletons)\)/) {
			$d{'scan_obj'} = $1;
			$d{'scan_doc'} = $2 + $3 + $4;
		}
		elsif (/\[scanner] Created (\d+) cards /) {
			$d{'scan_card'} = $1;
		}
		elsif (/\[chewer] ([^:]+): Generated (\d+) cards/) {
			$d{"chewer_card_$1"} = $2;
		}
		elsif (/\[merger] Merged (\d+) cards: (\d+) non-trivial classes \(max \d+\), (\d+) duplicates, (\d+) penalized/) {
			$d{'merge_card'} = $1;
			$d{'merge_classes'} = $2;
			$d{'merge_dupl'} = $3;
			$d{'merge_penal'} = $4;
		}
		elsif (/\[mklex] Built lexicon with (\d+) words/) {
			$d{'mklex_words'} = $1;
		}
		elsif (/\[sizer]/) {
			$d{'size_times'} = 1;
			if (/MD5.db=(\d+)/) { $d{'size_md5'} = $1; }
			if (/URL.db=(\d+)/) { $d{'size_url'} = $1; }
			if (/hosts=(\d+)/) { $d{'size_hosts'} = $1; }
			if (/objects=(\d+)/) { $d{'size_objs'} = $1; }
			if (/queue=(\d+)/) { $d{'size_queue'} = $1; }
			if (/card-attrs=(\d+)/) { $d{'size_attrs'} = $1; }
			if (/cards=(\d+)/) { $d{'size_cards'} = $1; }
			if (/lexicon=(\d+)/) { $d{"size_lexicon_$last_index"} = $1; }
			if (/references=(\d+)/) { $d{'size_refs'} = $1; }
			if (/string-map=(\d+)/) { $d{'size_string'} = $1; }
			if (/total index size is (\d+)/) { $d{"size_total_$last_index"} = $1; }
		}
		elsif (/\[ireport] Filetypes in: (.+)$/) {
			my %ft = map { split /=/, $_, 2 } split(/\s+/, $1);
			for my $i (keys %ft) {
				$d{"ft_in_" . $i} = $ft{$i} if $i !~ /^RFU\d+$/;
			}
		}
		elsif (/\[ireport] Filetypes out: (.+)$/) {
			my %ft = map { split /=/, $_, 2 } split(/\s+/, $1);
			for my $i (keys %ft) {
				$d{"ft_out_" . $i} = $ft{$i} if $i !~ /^RFU\d+$/;
			}
		}
		elsif (/\[ireport] Languages: (.+)$/) {
			my %lang = map { split /=/, $_, 2 } split(/\s+/, $1);
			for my $i (keys %lang) {
				$d{"lang_" . (($i eq '??') ? 'unknown' : $i)} = $lang{$i};
			}
		}

		if (!$sending && /\[send-indices]/) {
			$d{'idxcopy_start'} = $time;
			$sending = 1;
		}
		elsif ($sending && /\[scheduler] All indices send/) {
			$d{'idxcopy_end'} = $time;
		}
	}
	for my $k (sort keys %d) {
		defined $d{$k} and print $out "$start_time\t$k\t$d{$k}\n";
	}

	analyze_finish($in,$out,$infile,$lastpos);
}

