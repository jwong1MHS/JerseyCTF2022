#!/usr/bin/perl -w

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Expirer: actions";
$pic->{INIT_CMD}.="set yrange [0:]\nset data style linespoints\n";
my $e = $pic->new_plot("Expired");
my $r = $pic->new_plot("Requeued");
my $f = $pic->new_plot("Filtered out");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix,\&proc_line);

$pic->draw_picture;

sub proc_line {
	my %r = @_;
	$pic->plot_value($e, $r{'start_time'}, $r{'exp_spectrum_expired'}) if defined $r{'exp_spectrum_expired'};
	$pic->plot_value($r, $r{'start_time'}, $r{'exp_spectrum_requeued'}) if defined $r{'exp_spectrum_requeued'};
	$pic->plot_value($f, $r{'start_time'}, $r{'exp_spectrum_filtered'}) if defined $r{'exp_spectrum_filtered'};
}

