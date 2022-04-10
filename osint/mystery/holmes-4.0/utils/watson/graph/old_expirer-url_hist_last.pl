#!/usr/bin/perl -w

use lib "lib/perl5";
use Sherlock::Watsonlib;
use POSIX;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Expirer: URL histogram (last run)";
$pic->{INIT_CMD}.="set data style histeps\nset xdata\n";
my $n = $pic->new_plot("New");
my $r = $pic->new_plot("Refresh",$n,"1:3");
my $p = $pic->new_plot("Priority",$n,"1:4");
my $e = $pic->new_plot("Error",$n,"1:5");
my $s = $pic->new_plot("Static",$n,"1:6");

compute_stat($stat_begintime,$stat_endtime,$stat_prefix,\&proc_line);

my ($new,$rfsh,$prio,$err,$stat,$time);

sub proc_line {
	my %r = @_;

	defined $r{'exp_hist_new'} and $new = $r{'exp_hist_new'} and $time = $r{'start_time'};
	defined $r{'exp_hist_rfsh'} and $rfsh = $r{'exp_hist_rfsh'};
	defined $r{'exp_hist_prio'} and $prio = $r{'exp_hist_prio'};
	defined $r{'exp_hist_err'} and $err = $r{'exp_hist_err'};
	defined $r{'exp_hist_stat'} and $stat = $r{'exp_hist_stat'};
}

$pic->{INIT_CMD}.= "set xlabel \"Buckets (from ".strftime("%Y-%m-%d %H:%M:%S",localtime($time)).")\"\n";

my @new = split /,/, (defined $new ? $new : "");
my @rfsh = split /,/, (defined $rfsh ? $rfsh : "");
my @prio = split /,/, (defined $prio ? $prio : "");
my @err = split /,/, (defined $err ? $err : "");
my @stat = split /,/, (defined $stat ? $stat : "");

die "Inconzistency found in interfile" unless $#new==$#rfsh && $#new==$#prio && $#new==$#err && $#new==$#stat;

my $f = $pic->{PLOT_HANDLE}{$n};
for (my $i=0; $i<$#new; $i++) {
	print $f $i," ", $new[$i]," ",$rfsh[$i]," ",$prio[$i]," ",$err[$i]," ",$stat[$i],"\n";
}

$pic->draw_picture;

