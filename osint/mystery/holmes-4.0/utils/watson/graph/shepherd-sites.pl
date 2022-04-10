#!/usr/bin/perl -w
# Watson statistics
# (c) 2006 Pavel Charvat <pchar@ucw.cz>

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Shepherd: Sites";
$pic->{INIT_CMD}.="set yrange [0:]\nset data style linespoints\n";
my $a = $pic->new_plot("Alive");
my $u = $pic->new_plot("Unresolved");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix, sub {
        my %r = @_;
        $pic->plot_value($a, $r{'start_time'}, $r{'site_alive'}) if defined $r{'site_alive'};
        $pic->plot_value($u, $r{'start_time'}, $r{'site_unres'}) if defined $r{'site_unres'};
});

$pic->draw_picture;
