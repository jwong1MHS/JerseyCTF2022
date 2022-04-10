#!/usr/bin/perl -w
# Watson statistics
# (c) 2004 Tomas Valla <tom@ucw.cz>
# (c) 2006 Vladimir Jelen <vladimir.jelen@netcentrum.cz>
# (c) 2007 Pavel Charvat <pchar@ucw.cz>

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Indexer: documents";
$pic->{INIT_CMD}.="set yrange [0:]\nset data style linespoints\n";
my $a = $pic->new_plot("Cards on scanner output");
my $b = $pic->new_plot("Documents on scanner input");

my %known_index = ();

compute_stat($stat_begintime,$stat_endtime,$stat_prefix,\&proc_line);

$pic->draw_picture;

sub proc_line {
	my %r = @_;
	$pic->plot_value($a, $r{'start_time'}, $r{'scan_card'}) if defined $r{'scan_card'};
	$pic->plot_value($b, $r{'start_time'}, $r{'scan_doc'}) if defined $r{'scan_doc'};
	for my $k (keys %r) {
		if ($k =~ /chewer_card_(\w+)/) {
			defined $known_index{$1} or $known_index{$1} = $pic->new_plot("Cards on chewer output (index $1)");
			$pic->plot_value($known_index{$1}, $r{'start_time'}, $r{$k});
		}
	}
}
