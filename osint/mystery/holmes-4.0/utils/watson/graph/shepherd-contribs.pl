#!/usr/bin/perl -w
# Watson statistics
# (c) 2004 Tomas Valla <tom@ucw.cz>

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Shepherd: Contributions";
$pic->{INIT_CMD}.="set yrange [0:]\nset data style linespoints\n";
my $f = $pic->new_plot("Found");
my $n = $pic->new_plot("New");
my $r = $pic->new_plot("Recorded");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix, sub {
	my %r = @_;
	$pic->plot_value($f, $r{'start_time'}, $r{'contribs_found'}) if defined $r{'contribs_found'};
	$pic->plot_value($n, $r{'start_time'}, $r{'contribs_new'}) if defined $r{'contribs_new'};
	$pic->plot_value($r, $r{'start_time'}, $r{'contribs_recorded'}) if defined $r{'contribs_recorded'};
});

$pic->draw_picture;

