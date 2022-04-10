#!/usr/bin/perl -w
# Watson statistics
# (c) 2005 Vladimir Jelen <vladimir.jelen@netcentrum.cz>

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Indexer: Status";
$pic->{INIT_CMD}.="set yrange [0:1.3]\nset style fill solid 1\nset style data boxes\n";
my $a = $pic->new_plot("Stage 1");
my $b = $pic->new_plot("Stage 2");
my $c = $pic->new_plot("Index send");
my $d = $pic->new_plot("Shut down - manual");
my $e = $pic->new_plot("Shut down - error");

$pic->{PLOT_WITH}{1} .= "boxes lt 3 ";
$pic->{PLOT_WITH}{2} .= "boxes lt 5 ";
$pic->{PLOT_WITH}{3} .= "boxes lt 2 ";
$pic->{PLOT_WITH}{4} .= "boxes lt -1 ";
$pic->{PLOT_WITH}{5} .= "boxes lt 1 ";

compute_stat($stat_begintime,$stat_endtime,$stat_prefix, sub {
	my %r = @_;
	$pic->plot_value($a, $r{'start_time'}, $r{'stage1'}) if defined $r{'stage1'};
	$pic->plot_value($b, $r{'start_time'}, $r{'stage2'}) if defined $r{'stage2'};
	$pic->plot_value($c, $r{'start_time'}, $r{'sending'}) if defined $r{'sending'};
	$pic->plot_value($d, $r{'start_time'}, $r{'down'}) if defined $r{'down'};
	$pic->plot_value($e, $r{'start_time'}, $r{'error'}) if defined $r{'error'};
});

$pic->draw_picture;
