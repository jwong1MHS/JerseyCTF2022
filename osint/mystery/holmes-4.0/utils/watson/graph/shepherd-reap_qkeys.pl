#!/usr/bin/perl -w
# Watson statistics
# (c) 2006 Pavel Charvat <pchar@ucw.cz>

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Shepherd: Reaper QKeys";
$pic->{INIT_CMD}.="set yrange [0:]\nset data style linespoints\n";
my $m = $pic->new_plot("Remain");
my $r = $pic->new_plot("Avg ready");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix, sub {
        my %r = @_;
        $pic->plot_value($m, $r{'start_time'}, $r{'jobs_remain'}) if defined $r{'jobs_remain'};
        $pic->plot_value($r, $r{'start_time'}, $r{'jobs_avg_active'}) if defined $r{'jobs_avg_active'};
});

$pic->draw_picture;
