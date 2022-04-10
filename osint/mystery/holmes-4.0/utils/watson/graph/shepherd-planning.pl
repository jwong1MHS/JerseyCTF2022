#!/usr/bin/perl -w
# Watson statistics
# (c) 2004 Tomas Valla <tom@ucw.cz>

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Shepherd: Planning";
$pic->{INIT_CMD}.="set yrange [0:]\nset data style linespoints\n";
my $g = $pic->new_plot("Gathered URLs");
my $p = $pic->new_plot("Planned URLs");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix, sub {
	my %r = @_;
	$pic->plot_value($g, $r{'start_time'}, $r{'reap_gathered'}) if defined $r{'reap_gathered'};
	$pic->plot_value($p, $r{'start_time'}, $r{'reap_planned'}) if defined $r{'reap_planned'};
});

$pic->draw_picture;

