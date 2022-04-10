#!/usr/bin/perl -w
# Watson statistics
# (c) 2006 Pavel Charvat <pchar@ucw.cz>

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Shepherd: Reaper queues";
$pic->{INIT_CMD}.="set yrange [0:]\nset data style linespoints\n";
my $a = $pic->new_plot("Avg active");
my $p = $pic->new_plot("Avg prefetched");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix, sub {
        my %r = @_;
        $pic->plot_value($a, $r{'start_time'}, $r{'jobs_avg_active'}) if defined $r{'jobs_avg_active'};
        $pic->plot_value($p, $r{'start_time'}, $r{'jobs_avg_prefetched'}) if defined $r{'jobs_avg_prefetched'};
});

$pic->draw_picture;
