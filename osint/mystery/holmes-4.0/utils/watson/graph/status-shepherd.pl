#!/usr/bin/perl -w
# Watson statistics
# (c) 2005 Vladimir Jelen <vladimir.jelen@netcentrum.cz>

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Shepherd: Status";
$pic->{INIT_CMD}.="set yrange [0:1.3]\nset style fill solid 1\nset style data boxes\n";
my $a = $pic->new_plot("Gather");
my $b = $pic->new_plot("Sort url & plan");
my $c = $pic->new_plot("Database cleanup");
my $d = $pic->new_plot("Shut down - manual");
my $e = $pic->new_plot("Shut down - error");

my %share = (
	'gather' => 0,
	'plan' => 0,
	'cleanup' => 0,
	'down' => 0,
	'error' => 0,
);
my $last = '';
my $last_time = 0;
my $total_time = 0;

$pic->{PLOT_WITH}{1} .= "boxes lt 3 ";
$pic->{PLOT_WITH}{2} .= "boxes lt 5 ";
$pic->{PLOT_WITH}{3} .= "boxes lt 2 ";
$pic->{PLOT_WITH}{4} .= "boxes lt -1 ";
$pic->{PLOT_WITH}{5} .= "boxes lt 1 ";

compute_stat($stat_begintime,$stat_endtime,$stat_prefix, sub {
	my %r = @_;
	$share{"$last"} += $r{'start_time'} - $last_time if $last;
	$last_time = $r{'start_time'};
	if(defined $r{'gather'}) {
		$pic->plot_value($a, $r{'start_time'}, $r{'gather'});
		$last = 'gather' if $r{'gather'};
	}
	if(defined $r{'plan'}) {
		$pic->plot_value($b, $r{'start_time'}, $r{'plan'});
		$last = 'plan' if $r{'plan'};
	}
	if(defined $r{'cleanup'}) {
		$pic->plot_value($c, $r{'start_time'}, $r{'cleanup'});
		$last = 'cleanup' if $r{'cleanup'};
	}
	if(defined $r{'down'}) {
		$pic->plot_value($d, $r{'start_time'}, $r{'down'});
		$last = 'down' if $r{'down'};
	}
	if(defined $r{'error'}) {
		$pic->plot_value($e, $r{'start_time'}, $r{'error'});
		$last = 'error' if $r{'error'};
	}
});

foreach my $val(values %share) {
	$total_time += $val;
}

$total_time /= 100;
foreach my $key(keys %share) {
	$share{"$key"} /= $total_time;
	$share{"$key"} = sprintf('%5.1f', $share{"$key"});
}

$pic->{PLOT_TITLE}{1} .= ": $share{'gather'}%";
$pic->{PLOT_TITLE}{2} .= ": $share{'plan'}%";
$pic->{PLOT_TITLE}{3} .= ": $share{'cleanup'}%";
$pic->{PLOT_TITLE}{4} .= ": $share{'down'}%";
$pic->{PLOT_TITLE}{5} .= ": $share{'error'}%";

$pic->draw_picture;
