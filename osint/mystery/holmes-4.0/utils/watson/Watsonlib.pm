#
# Perl library module for Watson monitoring system
#
# (c) 2003-2004 Tomas Valla <tom@ucw.cz>
# (c) 2006 Vladimir Jelen <vladimir.jelen@netcentrum.cz>
# (c) 2007 Pavel Charvat <pchar@ucw.cz>
#

package Sherlock::Watsonlib;

require Exporter;
@ISA = qw(Exporter);
@EXPORT = qw(
$common_options_str $verbose $force $quantum $header_size parse_timestamp decompress_method
is_compressed get_headerinfo header_update newfile common_options
analyze_init analyze_finish
compute_stat logprint info warning stat_usage stat_options
analyze_options analyze_usage
$stat_prefix $stat_picname $stat_begintime $stat_endtime $stat_expanddays
read_config read_config_value read_config_array server_config_value $config_file %config
);

use strict;
use warnings;
use IO::File;
use Time::Local;
use Getopt::Long;
use POSIX;

our $gnuplot = "gnuplot";

our $force = 0;

our $verbose = 0;

our $quantum = 5*60;

our $header_size = 70;

our $error_time = 0;

########################################################
# Read config

our $config_file = "cf/watson";
our %config = ();

sub read_config {
	-f $config_file or die "Cannot open config file $config_file";
	my $cmd = ". $config_file >/dev/null 2>&1";
	foreach my $var (@_) {
		# A little complex import of bash variables from the configuration script ;-)
		# ... should work for any string values
		if ($var =~ /^[a-zA-Z_][a-zA-Z0-9_]*$/) { # A single varible
			$cmd.=' && echo -n "\$config{\"'.$var.'\"}='."'".'" && ' .
			'(echo -n "${'.$var.'}" | sed "s'."/'/'.\\\"'\\\".'/".'g") && ' .
			'echo "'."'".';"';
		}
		elsif ($var =~ /^([a-zA-Z_][a-zA-Z_0-9]*)\*$/) { # All variables with a given prefix
			$cmd.=' && (for VAR in ${!'.$1.'*} ; do ' . 
			'echo -n "\$config{\"$VAR\"}='."'".'" && ' .
			'(eval echo -n "\${$VAR}" | sed "s'."/'/'.\\\"'\\\".'/".'g") && ' .
			'echo "'."'".';"; done)';
		}
		else { die "Invalid read_config parameter"; }
	}
	eval `$cmd`;
}

sub read_config_value {
	my ($var, $default) = @_;
	defined $config{"$var"} or return $default;
	if ($config{"$var"} eq '') {
		defined $default or die "Missing required value $var in config file $config_file";
		return $default;
	}
	return $config{"$var"};
}

sub read_config_array {
	my ($var) = @_;
	return split(/\s+/, read_config_value($var, ""));
}

sub server_config_value {
	my ($server, $var_prefix, $var_default, $default) = @_;
	my $v = $default;
	if (defined($config{$var_default ? $var_default : $var_prefix})) {
		$v = $config{$var_default ? $var_default : $var_prefix};
	}
	my $x = $var_prefix;
	foreach my $y (split(/_/, $server)) {
		$x = $x."_".$y;
		if (defined($config{$x})) { $v = $config{$x}; }
        }
	return $v;
}
			    
########################################################
# Misc. funtions

sub analyze_options {
	GetOptions(
		"force!" => \$force,
		"quantum=i" => \$quantum,
		"verbose!" => \$verbose,
	) and $#ARGV==1
}

sub analyze_usage {
"Syntax: $0 [options] <logfile> <interfile>
Options:
--force		Force computing statistics (if they already exist)
--verbose	Verbosely print what's going on
--quantum sec	Round the statistics to given time quantum (in seconds)
		For special purposes only.
"
}

our ($stat_prefix,$stat_picname,$stat_begintime,$stat_endtime);
our $stat_expanddays = 1;
our $stat_tmpdel = 1;

sub stat_options {
	GetOptions(
		"quantum=i" => \$quantum,
		"verbose!" => \$verbose,
		"expand!" => \$stat_expanddays,
		"tmpdel!" => \$stat_tmpdel,
	) and $#ARGV==5 or return 0;

	$stat_prefix = $ARGV[0];
	$stat_picname = $ARGV[1];
	$stat_begintime = parse_timestamp($ARGV[2],$ARGV[3]);
	$stat_endtime = parse_timestamp($ARGV[4],$ARGV[5]);
	die "Bad timestamp" if $stat_begintime==-1 || $stat_endtime==-1;

	return 1;
}

sub stat_usage {
"Usage: $0 [options] <prefix> <picfile> <startdate> <starttime> <enddate> <endtime>
Options:
--verbose	Verbosely print what's going on
--quantum sec	Round the statistics to given time quantum (in seconds)
		For special purposes only.
--[no]expand	If noexpand specified, the date won't be appended to <prefix>,
		computation will hold only to the file <prefix>. Default is expand.\
--[no]tmpdel	If notmpdel specified, temporary files will not be deleted and
		their names will be printed instead.
"
}

sub parse_timestamp($$) {
	my ($date,$time) = @_;
	my $p=$date." ".$time;
			#              1y     2m     3d      4h     5m     6s
	return -1 unless $p =~ /^\s*(\d\d\d\d)-(\d{1,2})-(\d{1,2}) +(\d{1,2}):(\d{1,2}):(\d{1,2})\s*$/;
	return timelocal($6,$5,$4,$3,$2-1,$1-1900);
}

##############################################################
# Interfiles parsing

our @colnames = ();

sub compute_stat {
	my $begin = shift;
	my $end = shift;
	my $prefix = shift;
	my $func = shift;

	$begin-=$begin % $quantum;
	$end+= $quantum - ($end % $quantum);

	$error_time = $begin;

	foreach my $file ($stat_expanddays ? expand_days($begin,$end) : "") {

		next unless -e "$prefix$file";

		my $f = new IO::File "$prefix$file" or die "Can't open $prefix$file";
		my %row = ();
		my $l;
		my $file_type=1;
		my $last_time;
		my $count =0;

		# skip beginning of the file
		while($l=<$f>) {
			#$count++;
			#print "skipping $prefix$file [$count] $l";
			chomp $l;
			if ($l=~/^#@/) {
				$file_type=2;
				$last_time=0;
			} elsif ($l=~/^#&/) {
				$file_type=1;
				$l=~s/^#&\s+//;
				@colnames = split /\t/,$l;
			} elsif ($l=~/^#/) {
			} else {
				if ($file_type==1) {
					my $i = 0;
					%row = map { $colnames[$i++] => $_ } split /\t/,$l;
					last if $row{'start_time'}>=$begin;
				} else {
					last if (split /\t/,$l)[0]>=$begin;
				}
			}
		}

		# process lines with apropriate timestamp
		do {
			#print "chewing $prefix$file [$count] $l";
			goto OUTSIDE if not $l;
			chomp $l;
			if ($l=~/^#@/) {
				$file_type=2;
				$last_time=0;
			} elsif ($l=~/^#&/) {
				$file_type=1;
				$l=~s/^#&\s+//;
				@colnames = split /\t/,$l;
			} elsif ($l=~/^#/) {
			} else {
				if ($file_type==1) {
					my $i = 0;
					%row = map { $colnames[$i++] => $_ } split /\t/,$l;
					&$func(%row);
				} else {
					my @a = split /\t/,$l;
					if ($last_time < $a[0]) {
						$row{'start_time'} = $last_time;
						if ($last_time>0) {
							#print "calling func() with ",$row{'start_time'}," ",keys(%row),"\n";
							&$func(%row);
						}
						$last_time=$a[0];
						%row=( start_time => $last_time );
					}
					$row{$a[1]} = $a[2];
				}
			}
			#$count++;
		}while ($row{'start_time'}<=$end && ($l=<$f>));
		OUTSIDE:
		if ($file_type==2 && scalar(keys %row)>0 ) {
			#print "finishing block with ",keys(%row)," ",values(%row),"\n";
			&$func(%row);
		}
		$f->close;
	}
}

sub expand_days {
	my ($begin,$end) = @_;
	my @r=();
	my $b = $begin;
	my @p = localtime($end);
	my $k = sprintf("%04d%02d%02d",$p[5]+1900,$p[4]+1,$p[3]);
	my $day;

	do {
		my @a = localtime($b);
		$day = sprintf("%04d%02d%02d",$a[5]+1900,$a[4]+1,$a[3]);
		push @r, $day;
		$b+=24*60*60;
	} while ($day ne $k);

	return @r;
}

##################################################################
# Logging stuff

sub info {
	my $msg = shift;
	logprint("I",$msg) if $verbose;
}

sub warning {
	my $msg = shift;
	logprint("W",$msg);
}

sub logprint {
	my ($cat,$msg) = @_;
	my @a = localtime();
	pop @a;pop @a;pop @a;
	my $tm = sprintf("%04d-%02d-%02d %02d:%02d:%02d",reverse @a);
	print STDERR "$cat $tm [$0] $msg\n";
}

#################################################################
# Functions common in analyze-* scripts

sub analyze_init {
	my ($infile,$outfile,$hdr_string) = @_;
	my ($in, $out, $fname, $seekpos);
	$fname="";
	$seekpos=0;
	my $compressed=is_compressed($infile);

	info "Processing $infile into $outfile";
	if ($compressed) {
		info "Reading from compressed file";
		die "Can't read $infile" unless -r $infile;
		$in=new IO::File decompress_method($infile)."|" or die "Can't open pipe from $infile";
		$out=newfile($outfile,$hdr_string);
	} else {
		$in = new IO::File "<$infile" or die "Cannot open inputfile $infile";
		my $mode = (-e $outfile) ? "+<" : "+>";
		$out = new IO::File $mode.$outfile or die "Cannot open interfile $outfile";
		($fname,$seekpos) = get_headerinfo($out);
		if ($force || ($fname eq "" && $seekpos==0)) {  # bad header - we recreate whole interfile
			info "Recomputing interfile";
			$out->close;
			$out=newfile($outfile,$hdr_string);
		} else {
			info "Seeking to $seekpos";
			$in->seek($seekpos,0) or die "Troubles seeking in $infile to $seekpos";
		}
	}
	info "Last change in $outfile was from $fname, $seekpos";
	return ($in,$out,$fname,$seekpos);
}

sub analyze_finish {
	my ($in,$out,$infile,$lastpos) = @_;
	$in->close;
	info "Updating header and finishing";
	header_update($out,$infile,$lastpos);
	$out->close;
}

sub is_compressed {
	my $f = shift;
	if ($f=~/\.gz$/ || $f=~/\.bz2$/) {return 1}
	else {return 0}
}

# useful when reading only
sub decompress_method {
	my $f = shift;

	if ($f =~ /^(.*)\.gz$/) {
		return "zcat $f";
	} elsif ($f =~ /^(.*)\.bz2/) {
		return "bzcat $f";
	}
	return "cat $f";
}

sub get_headerinfo {
	my $f=shift;
	seek($f,0,0) or die "Can't seek";
	my $s=<$f>;
	if (!defined($s) || length($s)!=$header_size || $s!~/^#\**([^*]+)\*+(\d+)\**\n$/)
		{return ("",0)}

	seek($f,0,2); # seek to the EOF
	return ($1,$2);
}

sub header_update {
	my ($f, $filename, $seekpos) = @_;
	my $curr=tell($f) or die "Can't tell";

	seek($f,0,0) or die "Can't seek";
	my $s="#*$filename*$seekpos";

	# there may be some troubles with the \n length, assume it is 1
	$s.="*"x($header_size-1-length($s));

	print $f $s;
	seek($f,$curr,0) or die "Can't seek";
}

sub newfile {
	my ($outfile,$hdr_string) = @_;

	if (-e $outfile) {unlink($outfile) or die "Can't delete $outfile";}

	my $out= new IO::File "+>$outfile" or die "Cannot open interfile $outfile";

	print $out "#".("*" x ($header_size-2))."\n";
	print $out $hdr_string;
	return $out;
}

1;

##################################################################
# Graph drawing class

package Sherlock::Watsonlib::picture;
use strict;
use warnings;
use POSIX;

sub new {
	my $proto = shift;
	my $class = ref($proto) || $proto;
	my $self = {};
	my $picname = shift;
	my $pictitle = shift;

	my $nm = tmpnam();
	my $f = new IO::File ">$nm" or die "Can't open tempfile";
	$self->{CMD_FILE} = $nm;

	$self->{CMD_HANDLE} = $f;

	$self->{INIT_CMD} =
"set grid
set data style lines
set bmargin 3
set border 0
set xdata time
set timefmt \"%Y:%j:%H:%M\"
set terminal png
set missing \"?\"
set output \"$picname\"
";
	defined $pictitle and $self->{INIT_CMD} .= "set title \"$pictitle\"\n";

	$self->{PIC_NAME} = $picname;

	$self->{PLOT_FILE} = ();
	$self->{PLOT_HANDLE} = ();
	$self->{PLOT_CNT} = 0;
	$self->{PLOT_USING} = ();
	$self->{PLOT_TITLE} = ();
	$self->{PLOT_WITH} = ();
	$self->{PLOT_COMMAND} = "plot";

	return bless($self,$class);
}

sub DESTROY {
	my $self = shift;
	my %h = ();
	for my $v ( values %{$self->{PLOT_FILE}} ) {$h{$v}=""}
	if ($stat_tmpdel) {
		unlink keys %h;
		unlink $self->{CMD_FILE};
	} else {
		print "gnuplot commands: ",$self->{CMD_FILE},"\n";
		print "datafiles: ", join(" ",keys %h), "\n";
	}
}

sub draw_picture {
	my $self = shift;
	my $c;
	my $size = 0;

	my $f = $self->{CMD_HANDLE};
	print $f $self->{INIT_CMD};

	for (my $i=1; $i<=$self->{PLOT_CNT}; $i++) {
		if ($self->{PLOT_HANDLE}{$i}->tell == 0) {
			my $fh = $self->{PLOT_HANDLE}{$i};
			my $tm = strftime("%Y:%j:%H:%M", localtime($error_time));
			print $fh "$tm 0 0 0 0 0 0 0 0 0\n";
		}

		if (defined $c) {
			$c .= ", ";
		} else {
			$c = $self->{PLOT_COMMAND}." ";
		}
		$c .= "\"".$self->{PLOT_FILE}{$i}."\" using ".$self->{PLOT_USING}{$i}." title \"".$self->{PLOT_TITLE}{$i}."\""
			.(defined $self->{PLOT_WITH}{$i} ? "with ".$self->{PLOT_WITH}{$i} : "");
	}
	for (my $i=1; $i<=$self->{PLOT_CNT}; $i++) {
		$self->{PLOT_HANDLE}{$i}->close;
	}

	print $f "$c\n";
	$f->close;

	my $cmd = $gnuplot." ".$self->{CMD_FILE};
	`$cmd`;
	if ($?) {
		die "Error running gnuplot";
	}

}

sub new_plot {
	my $self = shift;
	my $title = shift;
	my $plotnum = shift;
	my $datafields = shift;

	my $i = ++$self->{PLOT_CNT};
	my $nm;
	my $f;
	if (defined $plotnum) {
		$nm = $self->{PLOT_FILE}{$plotnum};
		$f = $self->{PLOT_HANDLE}{$plotnum};
	} else {
		$nm = tmpnam();
		$f = new IO::File ">$nm" or die "Can't open tempfile";
	}
	$self->{PLOT_FILE}{$i} = $nm;
	$self->{PLOT_HANDLE}{$i} = $f;
	$self->{PLOT_USING}{$i} = (defined $datafields ? $datafields :"1:2");
	$self->{PLOT_TITLE}{$i} = "$title";

	return $i;
}

sub plot_value {
	my $self = shift;
	my $fno = shift;
	my $time = shift;
	my @data = @_;

	my $f = $self->{PLOT_HANDLE}{$fno};

	print $f strftime("%Y:%j:%H:%M", localtime($time));

	print $f " ".join(" ",@data)."\n";
}

1;
