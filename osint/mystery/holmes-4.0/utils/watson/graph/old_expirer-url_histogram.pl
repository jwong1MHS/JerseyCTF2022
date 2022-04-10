#!/usr/bin/perl -w

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Expirer: URL histogram";
$pic->{INIT_CMD}.="set data style points\nset logscale z\nset border 4095\nset grid xtics ytics ztics\n";
$pic->{PLOT_COMMAND} = "splot";
my $n = $pic->new_plot("New", undef, "1:2:3");
my $r = $pic->new_plot("Refreshed",$n,"1:2:4");
my $p = $pic->new_plot("Priority",$n,"1:2:5");
my $e = $pic->new_plot("Error",$n,"1:2:6");
my $s = $pic->new_plot("Static",$n,"1:2:7");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix,\&proc_line);

$pic->draw_picture;

sub proc_line {
	my %r = @_;

	if (defined $r{'exp_hist_new'} && defined $r{'exp_hist_rfsh'} && defined $r{'exp_hist_prio'}
			&& defined $r{'exp_hist_err'} && defined $r{'exp_hist_stat'}) {

		my @new = split /,/, $r{'exp_hist_new'};
		my @rfsh = split /,/, $r{'exp_hist_rfsh'};
		my @prio = split /,/, $r{'exp_hist_prio'};
		my @err = split /,/, $r{'exp_hist_err'};
		my @stat = split /,/, $r{'exp_hist_stat'};
		my $bn = scalar(@new);

		die "Inconzistency found in interfile histograms" if $bn!=@rfsh || $bn!=@prio || $bn!=@err || $bn!=@stat;

		for (my $i=0; $i<$bn; $i++) {
			$pic->plot_value($n, $r{'start_time'}, $i, $new[$i], $rfsh[$i], $prio[$i], $err[$i], $stat[$i]);
		}
	}
}

