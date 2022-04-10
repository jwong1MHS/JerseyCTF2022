#!/usr/bin/perl -w

#
# Dr. Watson -- user-friendly frontend to statistics
#
# (c) 2003-2004 Tomas Valla <tom@ucw.cz>
# (c) 2005-2006 Vladimir Jelen <vladimir.jelen@netcentrum.cz>
# (c) 2007-2009 Pavel Charvat <pchar@ucw.cz>

# How to create custom pages with graphs:
#
# As ordinary CGI script you can supply arguments to drwatson.cgi.
# Possible arguments:
#  gath_stats
#  shep_stats
#  search_stats
#  idx_stats
#  mux_stats
#  spray_stats		- write graph name into apropriate argument
#  servers 		- server prefixes to display, leave empty to select all servers
#  gath_all
#  exp_all
#  idx_all
#  search_all
#  mux_all
#  spray_all		- set to nonempty if you want to view all graphs
#  bdate
#  btime
#  edate
#  etime 		- date and time as in user defined time
#  force 		- force recomputing of graphs
#  time_input 		- should be one of lastday,lastweek,lastmonth,user
#  hide_office		- set to nonempty if you want to hide graph choosing screen
#  grid			- show graps in 2D grids (server x type)
#
# Example:
# I'd like to view statistics of cache efficiency of sherlock0 and gatherer performance
# and spectrum for last week.
#
# drwatson.cgi?hide_office=true&time_input=lastweek&gath_stats=gatherd-performance&gath_stats=gatherd-spectrum&search_pref=sherlock0/sherlockd-&search_stats=search-cache_efficiency
#
#

use lib 'lib/perl5';
use UCW::CGI;
use UCW::Ulimit;
use Sherlock::Watsonlib;
use POSIX;
use strict;

read_config("GATHERD_*", "SHEPHERD_*", "SEARCH_*", "INDEXER_*", "MUX_*", "SPRAY_*", "LEX_*");

my @gatherd_servers = read_config_array("GATHERD_SERVERS");
my @shepherd_servers = read_config_array("SHEPHERD_SERVERS");
my @search_servers = read_config_array("SEARCH_SERVERS");
my @indexer_servers = read_config_array("INDEXER_SERVERS");
my @mux_servers = read_config_array("MUX_SERVERS");
my @spray_servers = read_config_array("SPRAY_SERVERS");
my @lex_servers = read_config_array("LEX_SERVERS");
my @all_servers = (@gatherd_servers, @shepherd_servers, @search_servers, @indexer_servers, @mux_servers, @spray_servers);

my $graph_cgi_url = 'graph.cgi';

my $gth_stat_mask = "graph/gatherd-*";
my $idx_stat_mask = "graph/indexer-*";
my $shep_stat_mask = "graph/shepherd-*";
my $mux_stat_mask = "graph/mux-*";
my $srch_stat_mask = "graph/search-*";
my $spray_stat_mask = "graph/spray-*";

# Log file prefix & name of stat script
my $idx_stat_status = "status-indexer";
my $shep_stat_status = "status-shepherd";

my @gath_allstats = map { (reverse split "/",$_)[0] } glob($gth_stat_mask);
my @shep_allstats= map { (reverse split "/",$_)[0] } glob($shep_stat_mask);
my @search_allstats = map { (reverse split "/",$_)[0] } glob($srch_stat_mask);
my @indexer_allstats = map { (reverse split "/",$_)[0] } glob($idx_stat_mask);
my @mux_allstats = map { (reverse split "/",$_)[0] } glob($mux_stat_mask);
my @spray_allstats = map { (reverse split "/",$_)[0] } glob($spray_stat_mask);

push @shep_allstats, $shep_stat_status;
push @indexer_allstats, $idx_stat_status;
####

UCW::Ulimit::setlimit($UCW::Ulimit::AS, 40000000,40000000);

header("Dr. Watson");
body();
footer();

sub print_stats {
	my $stat_var = shift;
	my @r = ();

	foreach my $s (@_) {
		push @r, qq(<INPUT type="checkbox" name="$stat_var" value="$s">$s);
	}
	return @r;
}

sub header {
	my $title = shift;

	print <<END
Content-Type: text/html

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">
<HTML>
<HEAD>
<TITLE>$title</TITLE>
</HEAD>
<BODY>
END
	;
	$UCW::CGI::error_hook = sub {
		print "<H3>Error: $_[0]</H3>\n";
		print "</BODY></HTML>\n";
		die;
	};
}

sub footer {
	print "</BODY>\n</HTML>\n";
}

sub body {

	my ($bdate, $btime, $edate, $etime, $force, $time_input, $gath_all, $shep_all, $search_all, $idx_all, $mux_all, $spray_all, $hide_office, $grid);
	my (@gath_stats, @shep_stats, @search_stats, @servers, @idx_stats, @mux_stats, @spray_stats);
	my %pars = (
		gath_stats => {var => \@gath_stats},
		shep_stats => {var => \@shep_stats},
		search_stats => {var => \@search_stats},
		servers => {var => \@servers},
		idx_stats => {var => \@idx_stats},
		mux_stats => {var => \@mux_stats},
		spray_stats => {var => \@spray_stats},
		gath_all => {var => \$gath_all},
		shep_all => {var => \$shep_all},
		idx_all => {var => \$idx_all},
		search_all => {var => \$search_all},
		mux_all => {var => \$mux_all},
		spray_all => {var => \$spray_all},
		bdate => {var => \$bdate, check => '\d\d\d\d-\d{1,2}-\d{1,2}', default => '1990-01-01'},
		btime => {var => \$btime, check => '\d{1,2}:\d{1,2}', default => '00:00' },
		edate => {var => \$edate, check => '\d\d\d\d-\d{1,2}-\d{1,2}', default => strftime("%Y-%m-%d",localtime(time())) },
		etime => {var => \$etime, check => '\d{1,2}:\d{1,2}', default => '23:59' },
		force => {var => \$force},
		time_input => {var => \$time_input},
		hide_office => {var => \$hide_office},
		grid => {var => \$grid},
	);
	UCW::CGI::parse_args(\%pars);

	if (! $hide_office) {
		my $time_week_checked = ($time_input eq "lastweek") ? " CHECKED" : "";
		my $time_month_checked = ($time_input eq "lastmonth") ? " CHECKED" : "";
		my $time_user_checked = ($time_input eq "user") ? " CHECKED" : "";
		my $time_day_checked = ($time_week_checked || $time_month_checked || $time_user_checked) ? "" : " CHECKED";
		print <<END
<H1>Dr. Watson's office</H1>
<PRE style="float: right">
   \\\\\\\\
   c  oo
    | .U
   __=__                        ,,,
  |.  __|___                    oo ;
  ||_/  /  /                    U= _  0
  \\_/__/__E   o                 /. .| |
   (___ ||    |~~~~~~~~~~~~~~~~'----'~|
   I---|||    |-----------------------|
   I   |||    |       c(__)           |
   ^   '--''  ^                       ^
</PRE>
<FORM method="post" action="?" enctype="application/x-www-form-urlencoded">
<H2>Give me a term</H2>
<INPUT type="radio" name="time_input" value="lastday"$time_day_checked>Last day
<INPUT type="radio" name="time_input" value="lastweek"$time_week_checked>Last week
<INPUT type="radio" name="time_input" value="lastmonth"$time_month_checked>Last month
<BR>
<INPUT type="radio" name="time_input" value="user"$time_user_checked>User defined:
<BR>
<INPUT type="text" name="bdate" value="$bdate">
<INPUT type="text" name="btime" value="$btime">
Start date and time YYYY-MM-DD hh:mm<BR>

<INPUT type="text" name="edate" value="$edate">
<INPUT type="text" name="etime" value="$etime">
End date and time<BR>

<H2>What kind of examination would you like?</H2>
<TABLE border="1" cellspacing="0" cellpadding="3">
<CAPTION>Possible examinations</CAPTION>
<TR>
END
		;
		print '<TH>Gatherer' if @gatherd_servers;
		print '<TH>Shepherd' if @shepherd_servers;
		print '<TH>Search' if @search_servers;
		print '<TH>Indexer' if @indexer_servers;
		print '<TH>MUX' if @mux_servers;
		print '<TH>SPRAY' if @spray_servers;

		print '<TR>';
		print '<TD><INPUT type="checkbox" name="gath_all" value="x"' . ($gath_all?" CHECKED":"") . '>All' if  @gatherd_servers;
		print '<TD><INPUT type="checkbox" name="shep_all" value="x"' . ($shep_all?" CHECKED":"") . '>All' if @shepherd_servers;
		print '<TD><INPUT type="checkbox" name="search_all" value="x"' . ($search_all?" CHECKED":"") . '>All' if @search_servers;
		print '<TD><INPUT type="checkbox" name="idx_all" value="x"' . ($idx_all?" CHECKED":"") . '>All' if @indexer_servers;
		print '<TD><INPUT type="checkbox" name="mux_all" value="x"' . ($mux_all?" CHECKED":"") . '>All' if @mux_servers;
		print '<TD><INPUT type="checkbox" name="spray_all" value="x"' . ($spray_all?" CHECKED":"") . '>All' if @spray_servers;
		print "\n";

		my @gs = (@gatherd_servers ? reverse print_stats("gath_stats",@gath_allstats) : ());
		my @hs = (@shepherd_servers ? reverse print_stats("shep_stats",@shep_allstats) : ());
		my @is = (@indexer_servers ? reverse print_stats("idx_stats",@indexer_allstats) : ());
		my @ss = (@search_servers ? reverse print_stats("search_stats",@search_allstats) : ());
		my @ms = (@mux_servers ? reverse print_stats("mux_stats",@mux_allstats) : ());
		my @ps = (@spray_servers ? reverse print_stats("spray_stats",@spray_allstats) : ());

		while (scalar(@gs+@is+@hs+@ss+@ms+@ps)>0) {
			my $g = pop @gs if @gatherd_servers;
			my $h = pop @hs if @shepherd_servers;
			my $i = pop @is if @indexer_servers;
			my $s = pop @ss if @search_servers;
			my $m = pop @ms if @mux_servers;
			my $p = pop @ps if @spray_servers;
			print '<TR>';
			print '<TD>' . (defined $g ? $g : '') if @gatherd_servers;
			print '<TD>' . (defined $h ? $h : '') if @shepherd_servers;
			print '<TD>' . (defined $s ? $s : '') if @search_servers;
			print '<TD>' . (defined $i ? $i : '') if @indexer_servers;
			print '<TD>' . (defined $m ? $m : '') if @mux_servers;
			print '<TD>' . (defined $p ? $p : '') if @spray_servers;
			print "\n";
		}
		print "</TABLE>\n";

		print "<P>Statistics for servers:<BR>\n";
		{
			my %checked_hash = map { $_ => 1 } @servers;
			my %hash = ();
			foreach my $server (@all_servers) {
				my $x = "";
				foreach my $y (split(/_/, $server)) {
					$x = $x ? $x."_".$y : $y;
					$hash{$x} = undef;
				}
			}
			foreach my $prefix (sort keys %hash) {
				my $checked = $checked_hash{$prefix} ? " CHECKED" : "";
				print qq(<INPUT type="checkbox"name="servers" value="$prefix"$checked>$prefix<BR>\n);
			}
		}

		my $grid_checked = $grid ? " CHECKED" : "";
		print <<END
<P>
<INPUT type="checkbox"name="grid" value="x"$grid_checked>Grid display<BR>
<INPUT type="checkbox" name="force" value="force">Force recomputing<BR>
<HR>
<INPUT type="submit" name="action" value="Examine">
<INPUT type="reset">
</FORM>
END
		;
		foreach my $server (@lex_servers) {
			print qq(<P><A href="log/$server/">See word statistics for $server</A>);
		}
		print "<HR>\n";
	}

	if ($time_input eq "lastday") {
		my $t = time();
		$bdate = strftime("%Y-%m-%d",localtime($t-24*3600));
		$btime = strftime("%H-%M",localtime($t-24*3600));
		$edate = strftime("%Y-%m-%d",localtime($t));
		$etime = strftime("%H-%M",localtime($t));
	} elsif ($time_input eq "lastweek") {
		my $t = time();
		$bdate = strftime("%Y-%m-%d",localtime($t-7*24*3600));
		$btime = strftime("%H-%M",localtime($t-7*24*3600));
		$edate = strftime("%Y-%m-%d",localtime($t));
		$etime = strftime("%H-%M",localtime($t));
	} elsif ($time_input eq "lastmonth") {
		my $t = time();
		$bdate = strftime("%Y-%m-%d",localtime($t-30*24*3600));
		$btime = strftime("%H-%M",localtime($t-30*24*3600));
		$edate = strftime("%Y-%m-%d",localtime($t));
		$etime = strftime("%H-%M",localtime($t));
	} else {
		$btime =~ s/:/-/;
		$etime =~ s/:/-/;
	}

	@gath_stats = @gath_allstats if $gath_all;
	@shep_stats  = @shep_allstats if $shep_all;
	@search_stats = @search_allstats if $search_all;
	@idx_stats = @indexer_allstats if $idx_all;
	@mux_stats = @mux_allstats if $mux_all;
	@spray_stats = @spray_allstats if $spray_all;

	@servers = (@all_servers) if !@servers;
	my %matched_servers = ();
	{
		my %hash = map { $_ => 1 } @servers;
		foreach my $server (@all_servers) {
			my $x = "";
			foreach my $y (split(/_/, $server)) {
				$x = $x ? $x."_".$y : $y;
				if ($hash{$x}) { $matched_servers{$server} = 1; }
			}
		}
	}

	if(@gath_stats>0) {
		my @table = ();
		foreach my $server (@gatherd_servers) {
			if ($matched_servers{$server}) {
				my @row = ($server);
				my $prefix=server_config_value($server, "GATHERD_PREFIX", "", "gather-");
				foreach my $g (@gath_stats) {
					my $url="$graph_cgi_url?name=$g&prefix=$server/$prefix&bdate=$bdate&btime=$btime&edate=$edate&etime=$etime"
						.($force ? "&force=true" : "");
					push @row, $url;
				}
				push @table, \@row;
			}
		}
		table($grid, "Gatherer statistics", \@gath_stats, \@table);
	}

	if(@shep_stats>0) {
		my @table = ();
		foreach my $server (@shepherd_servers) {
			if ($matched_servers{$server}) {
				my @row = ($server);
				my $prefix=server_config_value($server, "SHEPHERD_PREFIX", "", "shepherd-");
				foreach my $s (@shep_stats) {
					my $tmp_prefix = ($s eq $shep_stat_status) ? "status-$prefix" : $prefix;
					my $url="$graph_cgi_url?name=$s&prefix=$server/$tmp_prefix&bdate=$bdate&btime=$btime&edate=$edate&etime=$etime"
						.($force ? "&force=true" : "");
					push @row, $url;
				}
				push @table, \@row;
			}
		}
		table($grid, "Shepherd statistics", \@shep_stats, \@table);
	}

	if(@idx_stats>0) {
		my @table = ();
		foreach my $server (@indexer_servers) {
			if ($matched_servers{$server}) {
				my @row = ($server);
				my $prefix=server_config_value($server, "INDEXER_PREFIX", "", "indexer-");
				foreach my $i (@idx_stats) {
					my $tmp_prefix = ($i eq $idx_stat_status) ? "status-$prefix" : $prefix;
					my $url="$graph_cgi_url?name=$i&prefix=$server/$tmp_prefix&bdate=$bdate&btime=$btime&edate=$edate&etime=$etime"
						.($force ? "&force=true" : "");
					push @row, $url;
				}
				push @table, \@row;
			}
		}
		table($grid, "Indexer statistics", \@idx_stats, \@table);
	}

	if(@search_stats>0) {
		my @table = ();
		foreach my $server (@search_servers) {
			if ($matched_servers{$server}) {
				my @row = ($server);
				my $prefix=server_config_value($server, "SEARCH_PREFIX", "", "sherlockd-");
				foreach my $s (@search_stats) {
					my $url="$graph_cgi_url?name=$s&prefix=$server/$prefix&bdate=$bdate&btime=$btime&edate=$edate&etime=$etime"
						.($force ? "&force=true" : "");
					push @row, $url;
				}
				push @table, \@row;
			}
		}
		table($grid, "Search statistics", \@search_stats, \@table);
	}

	if(@mux_stats>0) {
		my @table = ();
		foreach my $server (@mux_servers) {
			if ($matched_servers{$server}) {
				my @row = ($server);
				my $prefix=server_config_value($server, "MUX_PREFIX", "", "mux-");
				foreach my $m (@mux_stats) {
					my $url="$graph_cgi_url?name=$m&prefix=$server/$prefix&bdate=$bdate&btime=$btime&edate=$edate&etime=$etime"
						.($force ? "&force=true" : "");
					push @row, $url;
				}
				push @table, \@row;
			}
		}
		table($grid, "Mux statistics", \@mux_stats, \@table);
	}

	if(@spray_stats>0) {
		my @table = ();
		foreach my $server (@spray_servers) {
			if ($matched_servers{$server}) {
				my @row = ($server);
				my $prefix=server_config_value($server, "SPRAY_PREFIX", "", "spray-");
				foreach my $m (@spray_stats) {
					my $url="$graph_cgi_url?name=$m&prefix=$server/$prefix&bdate=$bdate&btime=$btime&edate=$edate&etime=$etime"
						.($force ? "&force=true" : "");
					push @row, $url;
				}
				push @table, \@row;
			}
		}
		table($grid, "Spray statistics", \@spray_stats, \@table);
	}
}

sub table {
	my ($grid, $header, $stats, $table) = (@_);
	if (@{$table} && @{$stats}) {
		print qq(<H3>$header</H3>\n) if $grid;
		print qq(<TABLE>\n) if $grid;
		for (my $row = 0; $row < @{$table}; $row++) {
			print qq(<H3>$header for $$table[$row][0]</H3>\n) if !$grid;
			print qq(<TR><TD>$$table[$row][0]\n) if $grid;
			for (my $col = 0; $col < @{$stats}; $col++) {
				print qq(<TD>) if $grid;
				print qq(<IMG src="$$table[$row][$col + 1]" alt="There should be graph $$stats[$col]");
				print qq(<BR>) if !$grid;
				print qq(\n);
			}
			print qq(\n);
		}
		print qq(</TABLE>\n) if $grid;
	}
}
