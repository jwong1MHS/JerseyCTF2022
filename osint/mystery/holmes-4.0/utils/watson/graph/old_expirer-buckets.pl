#!/usr/bin/perl -w

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Expirer: buckets";
$pic->{INIT_CMD}.="set yrange [0:]\nset data style linespoints\n";
my $t = $pic->new_plot("Total");
my $d = $pic->new_plot("Deleted");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix,\&proc_line);

$pic->draw_picture;

sub proc_line {
	my %r = @_;
	$pic->plot_value($t, $r{'start_time'}, $r{'exp_buck_total'}) if defined $r{'exp_buck_total'};
	$pic->plot_value($d, $r{'start_time'}, $r{'exp_buck_deleted'}) if defined $r{'exp_buck_deleted'};
}

