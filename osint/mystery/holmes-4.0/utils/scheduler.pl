#!/usr/bin/perl
#
#  Sherlock Gatherer Scheduler
#  (c) 2001--2002 Martin Mares <mj@ucw.cz>
#

use strict;
no strict 'vars';
use Config;
use POSIX;
use FileHandle;
use English;

# Scan signal numbers
$i=0;
foreach $name (split(' ', $Config{sig_name})) {
	$signo{$name} = $i++;
}

$timetable = 'cf/timetable';
if (@ARGV && $ARGV[0] =~ /\//) {
	$timetable = $ARGV[0];
	shift @ARGV;
}
@exceptions = ();
read_config();
$now = time;
$cycle = int(($now-$cycle_epoch)/$cycle_alignment);
$cycle_start_time = $cycle*$cycle_alignment + $cycle_epoch;
$slot = int(($now-$cycle_start_time)/$slot_time);
$slot_start_time = $cycle_start_time + $slot*$slot_time;
$shutdown = 0;
$shutdown_sends_alarm = 0;
$shutdown_done = 0;
$0 = $title if defined $title;

$SIG{QUIT} = $SIG{TERM} = sub {
	$shutdown = 1;
	msg("I", "Scheduler shutdown requested.");
	$shutdown_sends_alarm and kill $signo{ALRM}, $PID;
};

if (@ARGV) {
	if ($ARGV[0] eq "--whoami") {
		print "Cycle $cycle, starting on ", POSIX::strftime("%Y-%m-%d %H:%M:%S", localtime($cycle_start_time)), "\n";
		print "Slot $slot, starting on ", POSIX::strftime("%Y-%m-%d %H:%M:%S", localtime($slot_start_time)), "\n";
		exit 0;
	}
	@exceptions = @ARGV;
}

slot_start();
msg("I", "Scheduler starting ($cycle_slots slots each $slot_time sec, $action_count actions)");
if (@exceptions) {
	msg("I", "Requested out-of-order slots: " . join(' ', @exceptions));
}

while (!$shutdown_done) {
	if ($slot == $cycle_slots) {
		$slot = 0;
		$cycle++;
	}
	$slot = "DOWN" if $shutdown;
	slot_start();
	if (@exceptions) {
		$slot_name = shift @exceptions;
	} else {
		$slot_name = $slot;
	}
	if (open SF,">$slot_file") {
		print SF "$slot_name\n";
		close SF;
	}
	$slot_special = $shutdown || ($slot_name =~ "^$special_slots\$");
	msg("I", "Starting " . ($slot_special ? "special " : "") . "slot $slot_name of cycle $cycle.");
	$slot_start_time += $slot_time;
	if ($now >= $slot_start_time - $slot_min && !$slot_special) {
		msg("E", "Skipped whole slot, too short on time.");
	} else {
		for (my $i=0; $i<$action_count; $i++) {
			if ($slot_name =~ $action_guard[$i] ||
			    ($shutdown && $action_guard[$i] eq "^DOWN\$")) {
				my $term = 0;
				if ($action_term[$i]) {
					$term = $slot_start_time - time - $action_term[$i];
					if ($term <= $min_run_time || $shutdown) {
						msg("E", $action_shortname[$i] . " skipped, too tight on time.");
						next;
					}
				}
				run_cmd($action_cmd[$i], $term, $action_timeout[$i], $action_shortname[$i]);
			}
		}
		$now = time;
	}
	if ($slot eq "DOWN" && $shutdown) {
		msg("I", "Scheduler shut down.");
		$shutdown_done = 1;
	}
	send_report($progress_mail, "Scheduler status ($cycle:$slot_name)");
	if (!$shutdown && $now < $slot_start_time && !$slot_special) {
		my $dly = $slot_start_time - $now;
		msg("I", "Waiting $dly seconds for end of the slot.");
		interruptible_sleep($dly);
	}
	$slot++;
}
unlink $slot_file;

sub read_config {
	$action_count = 0;
	open CF, $timetable or die "Unable to open $timetable";
	while (<CF>) {
		chomp;
		/^\s*($|#)/ && next;
		if (/^(\$|@).*;\s*$/) {
			eval;
			$@ eq "" or die "$_: $@";
		} elsif (/^(\S+)\s+(\d+\+)?(\d+)\s+(\S.*)$/) {
			my $i = $action_count++;
			$action_guard[$i] = "^$1\$";
			$action_term[$i] = ($2 ne "") ? $2 : 0;
			$action_timeout[$i] = $3;
			$action_cmd[$i] = $4;
			if ($action_cmd[$i] =~ m/^{.*}$/) { $action_shortname[$i] = "Perl expression"; }
			elsif ($action_cmd[$i] =~ /^(\S+)/) { $action_shortname[$i] = $1; }
			else { $action_shortname[$i] = "???"; }
		} else {
			die "Configuration parse error: $_";
		}
	}
}

sub fatal {
	my ($msg) = @_;
	msg("!", $msg);
	send_report($error_mail, "ERROR: $msg");
	unlink $slot_file;
	exit 1;
}

sub msg {
	my ($level,$msg) = @_;
	my $time = POSIX::strftime "%Y-%m-%d %H:%M:%S", localtime;
	print STDERR "$level $time [scheduler] $msg\n";
}

sub send_report {
	my ($address, $subject) = @_;
	$address ne "" or return;
	open(MAIL, "|bin/send-mail -s '$subject' $address") or msg("E", "Unable to send mail with report!");
	print MAIL "Hello, Sherlock Scheduler speaking out its status report...\n\n";
	print MAIL "$subject\n\n";
	print MAIL "Host:                   " . `hostname -f`;
	print MAIL "Title:                  " . $title if defined $title;
	print MAIL "Current directory:      " . `pwd`;
	print MAIL "Timetable:              $timetable\n\n";
	print MAIL "Date and time           " . `date`;

	if (defined $log_slot_start && open(L, "<$current_logfile")) {
		seek L, $log_slot_start, 0;
		while (<L>) { print MAIL $_; }
		close L;
	}
	close MAIL;
}

sub run_cmd {
	my ($cmd, $term, $timeout, $shortname) = @_;
	if ($cmd =~ m/^{.*}$/) {
		msg("D", "Evaluating $cmd") if $trace;
		$cmd = eval $cmd;
		if ($cmd eq "") { return; }
		elsif ($cmd =~ /^(\S+)/) { $shortname = $1; }
	}
	msg("D", "Running '$cmd' term=$term timeout=$timeout") if $trace;
	my $pid = fork;
	defined $pid or fatal "fork failed: $!";
	if (!$pid) {
		my $exec = ($cmd !~ "[|&;()<>]") ? "exec " : "";
		exec("/bin/sh", "-c", "$exec$cmd") or exit 127;
	}
	local $SIG{ALRM} = sub {
		if ($term) {
			$shutdown_sends_alarm = 0;
			$term = 0;
			msg("D", "Sending SIGTERM") if $trace;
			kill $signo{TERM}, $pid;
			alarm $timeout;
		} else {
			fatal("$shortname: Command timed out");
		}
	};
	if ($term) { $shutdown_sends_alarm = 1; }
	alarm ($term ? $term : $timeout);
	my $p = wait;
	alarm 0;
	$shutdown_sends_alarm = 0;
	($p == $pid) || fatal("Wait returned unknown pid $p");
	if ($?) {
		my $msg = "unknown error status $?";
		if (POSIX::WIFEXITED($?)) { $msg = "error code " . POSIX::WEXITSTATUS($?); }
		elsif (POSIX::WIFSIGNALED($?)) { $msg = "signal " . POSIX::WTERMSIG($?); }
		fatal("$shortname: Command exited with $msg");
	}
}

sub slot_start {
	$now = time;
	if (defined $logfile) {
		$current_logfile = POSIX::strftime($logfile, localtime);
		open LOG, ">>$current_logfile" or fatal("Unable to open log file $current_logfile!");
		close STDERR;
		open STDERR, ">&LOG" or fatal("Unable to redirect stderr!");
		autoflush STDERR 1;
		close STDOUT;
		open STDOUT, ">&LOG" or fatal("Unable to redirect stdout!");
		autoflush STDOUT 1;
		close LOG;
		seek(STDERR,0,2);
		$log_slot_start = tell STDERR;
	}
}

sub interruptible_sleep {
	# I'd never believe how much work it is to implement sleep(). The usual
	# textbook solution with alarm() and pause() is wrong (racy).
	my $delay = shift @_;
	my $sigset = POSIX::SigSet->new( &POSIX::SIGALRM );
	my $oldsigset = POSIX::SigSet->new;
	my $caught = 0;
	local $SIG{ALRM} = sub { $caught = 1; };
	sigprocmask(POSIX::SIG_BLOCK, $sigset, $oldsigset);
	alarm($delay);
	$shutdown_sends_alarm = 1;
	while (!$caught) {
		sigsuspend($oldsigset);
	}
	$shutdown_sends_alarm = 0;
	sigprocmask(POSIX::SIG_SETMASK, $oldsigset, $sigset);
}
