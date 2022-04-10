#!/usr/bin/perl -w
# Watson statistics
# (c) 2004 Tomas Valla <tom@ucw.cz>

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Gatherer: Performance";
my $u = $pic->new_plot("URLs/sec");
$pic->new_plot("Succ. downloads/sec",$u,"1:3");
$pic->new_plot("Succ. resolves/sec",$u,"1:4");
$pic->new_plot("Errors/sec",$u,"1:5");

my ($url,$redir,$hard_err) = (0) x 4;
my ($first_time, $last_time);

compute_stat($stat_begintime,$stat_endtime,$stat_prefix,\&proc_line);

my $div = defined $last_time && defined $first_time ? ($last_time-$first_time) / (24*3600) : 1;
$url = int($url/$div);
$redir = int($redir/$div);
$hard_err = int($hard_err/$div);
$pic->{INIT_CMD} .= "set label \"Averages per day: $url urls, $redir redirects,\\n $hard_err errors\" at screen 0.10,0.9\n";

$pic->draw_picture;

sub proc_line {
	my %r = @_;
	my $e;

	$first_time = $r{'start_time'} unless defined $first_time;
	$last_time = $r{'start_time'};
	$url += (defined $r{'total'}) ? $r{'total'} : 0;
	$redir += (defined $r{'redirect'}) ? $r{'redirect'} : 0;
	$hard_err += (defined $r{'err_hard'}) ? $r{'err_hard'} : 0;

	$e = ((defined $r{'err_soft'}) ? $r{'err_soft'} : 0) + ((defined $r{'err_hard'}) ? $r{'err_hard'} : 0);

	$pic->plot_value($u, $r{'start_time'},
			(defined $r{'total'} ? $r{'total'} : 0)/$quantum,
			(defined $r{'download'} ? $r{'download'} : 0)/$quantum,
			(defined $r{'resolve'} ? $r{'resolve'} : 0)/$quantum,
			$e/$quantum);
}

