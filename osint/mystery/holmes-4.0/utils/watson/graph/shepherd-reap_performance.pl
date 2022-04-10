#!/usr/bin/perl -w
# Watson statistics
# (c) 2004 Tomas Valla <tom@ucw.cz>

use lib "lib/perl5";
use Sherlock::Watsonlib;
use strict;
use warnings;

stat_options() or die stat_usage();

my $pic = new Sherlock::Watsonlib::picture $stat_picname, "Shepherd: Reaper performace";
$pic->{INIT_CMD}.="set yrange [0:]\nset data style linespoints\n";
my $e = $pic->new_plot("Total jobs/sec");

my %known_reaper = ();

compute_stat($stat_begintime,$stat_endtime,$stat_prefix,\&proc_line);

$pic->draw_picture;

sub proc_line {
	my %r = @_;
	$pic->plot_value($e, $r{'start_time'}, $r{'jobs_sec'}) if defined $r{'jobs_sec'};
	for my $k (keys %r) {
		if ($k =~ /jobs_sec_reaper_(\w+)/) {
			defined $known_reaper{$1} or $known_reaper{$1} = $pic->new_plot("$1 jobs/sec");
			$pic->plot_value($known_reaper{$1}, $r{'start_time'}, $r{$k});
		}
	}
}

