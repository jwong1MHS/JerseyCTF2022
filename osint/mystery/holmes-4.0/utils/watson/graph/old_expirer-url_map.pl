#!/usr/bin/perl -w

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Expirer: URL map";
$pic->{INIT_CMD}.="set yrange [0:]\nset data style linespoints\n";
my $t = $pic->new_plot("Total URL's");
my $n = $pic->new_plot("Queued new");
my $r = $pic->new_plot("Queued refresh");
my $s = $pic->new_plot("Static");
my $e = $pic->new_plot("Error");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix,\&proc_line);

$pic->draw_picture;

sub proc_line {
	my %r = @_;
	$pic->plot_value($t, $r{'start_time'}, $r{'exp_total'}) if defined $r{'exp_total'};
	$pic->plot_value($n, $r{'start_time'}, $r{'exp_queued_new'}) if defined $r{'exp_queued_new'};
	$pic->plot_value($r, $r{'start_time'}, $r{'exp_queued_rfsh'}) if defined $r{'exp_queued_rfsh'};
	$pic->plot_value($s, $r{'start_time'}, $r{'exp_static'}) if defined $r{'exp_static'};
	$pic->plot_value($e, $r{'start_time'}, $r{'exp_error'}) if defined $r{'exp_error'};
}

