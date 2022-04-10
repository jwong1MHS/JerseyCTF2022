#!/usr/bin/perl -wT

# Simple graph accessing CGI script
#
# (c) 2003 Tomas Valla <tom@ucw.cz>
#

# Script arguments:
# name		name of statistics
# bdate		beginning and end time of computation
# btime			YYYY-MM-DD hh-mm
# edate
# etime
# prefix	interfile prefix
# options	list of arguments passed to statistic (optional)
# force		forces redrawing picture even if already in cache (optional)

use lib 'lib/perl5';
use UCW::CGI;
use UCW::Ulimit;
use strict;
use warnings;
use Time::Local;
use POSIX;

# this should be reconfigured
my $picdir="cache/graph/";
my $bindir="graph/";
my $intdir="log/inter/";

my $error_pic = "error.png";

$ENV{'PATH'} = "/opt/bin:/usr/local/bin:/usr/bin";

UCW::Ulimit::setlimit($UCW::Ulimit::AS, 40000000,40000000);

get_stat();

sub parse_timestamp($$) {
        my ($date,$time) = @_;
        my $p=$date." ".$time;
        return 0 unless $p =~ /^\s*(\d\d\d\d)-(\d{1,2})-(\d{1,2}) +(\d{1,2})-(\d{1,2})\s*$/;
        return timelocal(0,$5,$4,$3,$2-1,$1-1900);
}

sub get_stat {
	my ($name,$bdate,$btime,$edate,$etime,$prefix,$force,$options);
	my %pars = (
		name => { var => \$name, check => '[a-zA-Z0-9_\-]+' },
		bdate => { var => \$bdate, check => '\d+-\d+-\d+' },
		btime => { var => \$btime, check => '\d+-\d+' },
		edate => { var => \$edate, check => '\d+-\d+-\d+' },
		etime => { var => \$etime, check => '\d+-\d+' },
		prefix => { var => \$prefix },
		force => { var => \$force, default => 0 },
		options => { var => \$options, default => "" },
	);
	UCW::CGI::parse_args(\%pars);
	if ($prefix =~ /\.\./) {
		response("Forbidden prefix \"".$prefix."\"");
	}

	my $begin=parse_timestamp($bdate, $btime);
	my $end=parse_timestamp($edate, $etime);
	if (!$begin || !$end) {
		response("Invalid date\n");
	}

	my $s = $picdir.picture_name($name, $prefix, $begin,$end);

	if (! -f $s || $force) {
		my $tmpname = $picdir.$$;
		run_stat($bindir.$name, $options, $intdir.$prefix, $tmpname, $begin,$end);
		rename($tmpname,$s);
	}
	return_pic($s);
}

sub picture_name {
	my ($stat,$prefix,$begin,$end) = @_;
	$prefix=~s:/:_:g;
	$stat.".$prefix.".strftime("%Y%m%d%H%M",localtime($begin))."-".strftime("%Y%m%d%H%M",localtime($end)).".png";
}

sub return_pic {
	my $picname = shift;

	print "Content-Type: image/png\n\n";
	system("/bin/cat $picname");
}

sub run_stat {
	my ($stat,$options,$prefix,$pic,$begin,$end) = @_;

	if (-x $stat) {
		my $t = "$options $prefix $pic ".strftime("%Y-%m-%d %H:%M:%S",localtime($begin))
			." ".strftime("%Y-%m-%d %H:%M:%S",localtime($end));
		my @d = split(" ",$t);
		if (system($stat,@d)) {
			unlink($pic) if -f $pic;
			response("Error running $stat $t\n");
		}
	} else {
		response("There is nothing like $stat. Sorry.\n");
	}
}

# Reserved for the great days, when browsers will be capable of displaying
# both HTML and images in OBJECT tags.
# Currently returns some error picture.

sub response {
	my $resp = shift;

# 	print <<EOF
# Content-Type: text/html
#
# <!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">
# <HTML>
# <HEAD>
# <TITLE>graph.cgi response</TITLE>
# </HEAD>
# <BODY>
# EOF
# 	;
# 	print $resp;
# 	print "</BODY>\n</HTML>\n";

	return_pic($error_pic);

	exit 0;
}
