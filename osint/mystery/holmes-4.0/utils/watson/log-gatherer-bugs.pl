#!/usr/bin/perl -w
#
# Joins gatherer errors with reapd error messages and show mux errors
#
# (c) 2005 Vladimir Jelen <vladimir.jelen@netcentrum.cz>
# (c) 2007 Pavel Charvat <pchar@ucw.cz>
#
# usage: bin/log-gatherer-bugs <gatherer-log-files> <mux-log-files>

use strict;
use Time::Local;
use Getopt::Long;
use File::stat;
use File::Glob;

use lib 'lib/perl5';
use Sherlock::Watsonlib;

my $now = time;

# Support for opening compressed files
sub open_method ($) {
  $_ = shift;
  my $retval;
  if(/\.gz$/) { $retval = "zcat $_ |"; }
  elsif(/\.bz2$/) { $retval = "bzcat $_ |"; }
  else { $retval = $_; }
  return $retval;
}

my $force = 0;
GetOptions("force!" => \$force);

# Read config
read_config("LOG_LIST", "GATHERD_*", "REAP_*", "MUX_*");
my $listfile = read_config_value("LOG_LIST", "log/watson-error-list");
my @gatherd_servers = split(/\s+/, read_config_value("GATHERD_SERVERS", ""));
my @reapd_servers = split(/\s+/, read_config_value("REAP_SERVERS", ""));
my @mux_servers = split(/\s+/, read_config_value("MUX_SERVERS", ""));

# Read list of analyzed logs
my %analyzed = ();
if (open(LISTFILE, $listfile)) {
  while(<LISTFILE>) {
    chomp;
    $analyzed{$_} = 1;
  }
  close LISTFILE;
}
else {
  printf "Watson: cannot open $listfile, creating a new file\n";
}
open(LISTFILE, '>>', $listfile) or die "Cannot append file $listfile: $!";

# Analyze gatherer logs
my %errors = ();
my @goutput = ();
my %parse_reapers = map { $_ => {} } (@reapd_servers);
foreach my $server (@gatherd_servers) {
  my $prefix = server_config_value($server, "GATHERD_PREFIX", "", "gather-");
  my %reapers = ();
  foreach my $link (split(/\s+/, server_config_value($server, "GATHERD_REAPERS", "", ""))) {
    if ($link =~ /^([^:]+):(.+)$/) {
      $reapers{$1} = $2;
    }
  }
ANALYZE_GATHERD: foreach my $logfile (glob("log/$server/$prefix*")) {
    my $norm = $logfile;
    if ($logfile =~ /^(.+)(\.(gz|bz2))$/) { $norm = $1; }
    if(!$force) {
      my $tmp = stat $logfile or next ANALYZE_GATHERD;
      if($tmp->mtime > ($now - 86400)) { next ANALYZE_GATHERD; }
      if ($analyzed{$norm}) { next ANALYZE_GATHERD; }
    }
    open(LOGFILE, open_method($logfile)) or die "Cannot open file $logfile: $!";
    while(<LOGFILE>) {
      chomp;
      if(/^I (\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2}) \S+ [12]301 Gatherer bug \[([^:]+):([^*\]]+)/) {
        if (!defined($reapers{$7})) { die "Invalid GATHERD_REAPERS, cannot translate '$7' for server '$server'"; }
        else {
          my $reaper = $reapers{$7};
          if (!defined($parse_reapers{$reaper})) { die "Invalid REAP_SERVERS, missing '$reaper'"; }
          else {
            my $date = timegm($6, $5, $4, $3, $2 - 1, $1);
            $errors{$8} = [] if not defined $errors{$8};
            push @{$errors{$8}}, { 'id' => $8, 'rserv' => $reaper, 'date' => $date, 'txt' => $_ };
            $parse_reapers{$reaper}->{"$1$2$3"} = undef;
          }
        }
      }
    }
    close LOGFILE;
    if(!$force) { print LISTFILE "$norm\n"; }
  }
}

# Analyze & join reapd logs
foreach my $server (@reapd_servers) {
  my $prefix = server_config_value($server, "REAP_PREFIX", "", "reapd-");
  my $x = $parse_reapers{$server};
  foreach my $t (keys %$x) {
    my $logfile = "log/$server/$prefix$t";
    if (open(LOGFILE, open_method($logfile)) or open(LOGFILE, open_method($logfile . '.gz')) or open(LOGFILE, open_method($logfile . '.bz2'))) {
      while(<LOGFILE>) {
        chomp;
        if(/^. (\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2}) \[0+([0-9a-fA-F]+)/) {
          if(defined $errors{$7}) {
            my $date = timegm($6, $5, $4, $3, $2 - 1, $1);
            foreach my $gath (@{$errors{$7}}) {
              my $rs = $$gath{'rserv'};
              next if abs($$gath{'date'} - $date) > 120;
              next if not ($server eq $rs);
              $$gath{'reap'} = [] if not defined $$gath{'reap'};
              push @{$$gath{'reap'}}, $_;
            }
          }
        }
      }
      close LOGFILE;
    }
  }
}

# Analyze mux logs
my @moutput = ();
foreach my $server (@mux_servers) {
  my $prefix = server_config_value($server, "MUX_PREFIX", "", "mux-");
ANALYZE_MUX: foreach my $logfile (glob("log/$server/$prefix*")) {
    my $norm = $logfile;
    if ($logfile =~ /^(.+)(\.(gz|bz2))$/) { $norm = $1; }
    if(!$force) {
      my $tmp = stat $logfile or next ANALYZE_MUX;
      if($tmp->mtime > ($now - 86400)) { next ANALYZE_MUX; }
      if ($analyzed{$norm}) { next ANALYZE_MUX; }
    }
    open(LOGFILE, open_method($logfile)) or die "Cannot open file $logfile: $!";
    while(<LOGFILE>) {
      chomp;
      if (/^[E!]/) {
        push @moutput, $_;
      }
    }
    close LOGFILE;
    if(!$force) { print LISTFILE "$norm\n"; }
  }
}

close LISTFILE;

#Group results
foreach my $data (values %errors) {
  push @goutput, @$data;
}

# Print results
if(@goutput > 0) {
    print "Gatherer errors\n" . "-" x 80 . "\n\n";
    foreach $_ (sort { $$a{'date'} <=> $$b{'date'} } @goutput) {
    print $$_{'txt'} . "\n";
    foreach my $reap_txt (@{$$_{'reap'}}) {
      print $reap_txt . "\n";
    }
    print "\n";
  }
  print "\n\n";
}

if(@moutput > 0) {
  print "Mux errors\n" . "-" x 80 . "\n\n";
  $" = "\n";
  print "@moutput\n";
}
