#!/usr/bin/perl -w
# Watson statistics
# (c) 2004 Tomas Valla <tom@ucw.cz>

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Shepherd: Average sizes";
$pic->{INIT_CMD}.="set yrange [0:]\nset data style linespoints\n";
my $a = $pic->new_plot("Active");
my $e = $pic->new_plot("Estimated active");
my $i = $pic->new_plot("Inactive");
my $j = $pic->new_plot("Estimated inactive");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix, sub {
	my %r = @_;
	$pic->plot_value($a, $r{'start_time'}, $r{'size_active'}) if defined $r{'size_active'};
	$pic->plot_value($e, $r{'start_time'}, $r{'size_active_estim'}) if defined $r{'size_active_estim'};
	$pic->plot_value($i, $r{'start_time'}, $r{'size_inactive'}) if defined $r{'size_inactive'};
	$pic->plot_value($j, $r{'start_time'}, $r{'size_inactive_estim'}) if defined $r{'size_inactive_estim'};
});

$pic->draw_picture;

