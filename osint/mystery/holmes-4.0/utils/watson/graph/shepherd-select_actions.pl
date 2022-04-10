#!/usr/bin/perl -w
# Watson statistics
# (c) 2004 Tomas Valla <tom@ucw.cz>

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Shepherd: Select: Actions";
$pic->{INIT_CMD}.="set yrange [0:]\nset data style linespoints\n";
my $w = $pic->new_plot("Woken up");
my $l = $pic->new_plot("Lulled");
my $g = $pic->new_plot("Lost gathered");
my $d = $pic->new_plot("Discarded");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix, sub {
	my %r = @_;
	$pic->plot_value($w, $r{'start_time'}, $r{'act_woken_up'}) if defined $r{'act_woken_up'};
	$pic->plot_value($l, $r{'start_time'}, $r{'act_lulled'}) if defined $r{'act_lulled'};
	$pic->plot_value($g, $r{'start_time'}, $r{'act_lost_gathered'}) if defined $r{'act_lost_gathered'};
	$pic->plot_value($d, $r{'start_time'}, $r{'act_discarded'}) if defined $r{'act_discarded'};
});

$pic->draw_picture;

