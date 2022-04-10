#!/usr/bin/perl -w
# Watson statistics
# (c) 2004 Tomas Valla <tom@ucw.cz>
# (c) 2006 Pavel Charvat <pchar@ucw.cz>

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Shepherd: Select: URLs";
$pic->{INIT_CMD}.="set yrange [0:]\nset data style linespoints\n";
my $s = $pic->new_plot("New");
my $g = $pic->new_plot("Gathered");
my $a = $pic->new_plot("Active");
my $i = $pic->new_plot("Inactive");
my $z = $pic->new_plot("Zombie");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix, sub {
	my %r = @_;
	$pic->plot_value($s, $r{'start_time'}, $r{'url_new'}) if defined $r{'url_new'};
	$pic->plot_value($g, $r{'start_time'}, $r{'url_gathered'}) if defined $r{'url_gathered'};
	$pic->plot_value($a, $r{'start_time'}, $r{'url_active'}) if defined $r{'url_active'};
	$pic->plot_value($i, $r{'start_time'}, $r{'url_inactive'}) if defined $r{'url_inactive'};
	$pic->plot_value($z, $r{'start_time'}, $r{'url_zombie'}) if defined $r{'url_zombie'};
});

$pic->draw_picture;

