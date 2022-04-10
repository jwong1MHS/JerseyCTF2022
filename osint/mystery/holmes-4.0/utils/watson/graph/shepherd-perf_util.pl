#!/usr/bin/perl -w
# Watson statistics
# (c) 2004 Tomas Valla <tom@ucw.cz>

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Shepherd: Performance utilization";
$pic->{INIT_CMD}.="set yrange [0:]\nset data style linespoints\n";
my $l = $pic->new_plot("Performance limit");
my $u = $pic->new_plot("Performance utilized");
my $f = $pic->new_plot("Perf. limit freq. refresh");
my $r = $pic->new_plot("Perf. utilized freq. refresh");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix, sub {
	my %r = @_;
	$pic->plot_value($l, $r{'start_time'}, $r{'perf_limit'}) if defined $r{'perf_limit'};
	$pic->plot_value($u, $r{'start_time'}, $r{'perf_utilized'}) if defined $r{'perf_utilized'};
	$pic->plot_value($f, $r{'start_time'}, $r{'perf_limit_fr'}) if defined $r{'perf_limit_fr'};
	$pic->plot_value($r, $r{'start_time'}, $r{'perf_utilized_fr'}) if defined $r{'perf_utilized_fr'};
});

$pic->draw_picture;

