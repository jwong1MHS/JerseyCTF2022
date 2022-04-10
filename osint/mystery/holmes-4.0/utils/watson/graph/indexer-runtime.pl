#!/usr/bin/perl -w
# Watson statistics
# (c) 2004 Tomas Valla <tom@ucw.cz>
# (c) 2006 Vladimir Jelen <vladimir.jelen@netcentrum.cz>

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Indexer: running time";
$pic->{INIT_CMD}.="set yrange [0:]\nset data style linespoints\n";
my $t = $pic->new_plot("Running time (hours)");
my $c = $pic->new_plot("Index copy time (hours)");
my $indexing_start;
my $idxcopy_start;

compute_stat($stat_begintime,$stat_endtime,$stat_prefix,\&proc_line);

$pic->draw_picture;

sub proc_line {
	my %r = @_;

	$idxcopy_start = $r{'idxcopy_start'} if defined $r{'idxcopy_start'};
	$indexing_start = $r{'indexing_start'} if defined $r{'indexing_start'};
	
	if ((defined $idxcopy_start) && (defined $r{'idxcopy_end'})) {
		$pic->plot_value($c, $r{'start_time'}, ($r{'idxcopy_end'} - $idxcopy_start)/3600);
		$idxcopy_start = undef;
	}

	if ((defined $indexing_start) && (defined $r{'indexing_end'})) {
		$pic->plot_value($t, $r{'start_time'}, ($r{'indexing_end'} - $indexing_start)/3600);
		$indexing_start = undef;
	}
}

