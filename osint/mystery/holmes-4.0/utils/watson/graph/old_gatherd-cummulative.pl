#!/usr/bin/perl -w

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Gatherer: Cummulative statistics";
my $t = $pic->new_plot("Total URL's");
$pic->new_plot("Total refreshes",$t,"1:3");
$pic->new_plot("Total redirects",$t,"1:4");
$pic->new_plot("Total duplicates",$t,"1:5");
$pic->new_plot("Total hard errors",$t,"1:6");

my ($url,$refr,$redir,$dupl,$hard_err) = (0) x 5;

compute_stat($stat_begintime,$stat_endtime,$stat_prefix,\&proc_line);

$pic->{INIT_CMD} .= "set label \"$url urls, $refr refreshed,\\n$redir redirects, $dupl duplicates,\\n$hard_err errors\" at screen 0.15,0.9\n";
$pic->draw_picture;

sub proc_line {
	my %r = @_;

	$url += defined $r{'total'} ? $r{'total'} : 0;
	$refr += defined $r{'refresh'} ? $r{'refresh'} : 0;
	$redir += defined $r{'redirect'} ? $r{'redirect'} : 0;
	$dupl += defined $r{'duplicate'} ? $r{'duplicate'} : 0;
	$hard_err += defined $r{'err_hard'} ? $r{'err_hard'} : 0;

	$pic->plot_value($t, $r{'start_time'}, $url, $refr, $redir, $dupl, $hard_err);
}

