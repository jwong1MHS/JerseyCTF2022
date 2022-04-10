#!/bin/bash
# A simple search server benchmarking tool
# (c) MMV MM

set -e

SH=~sherlock/run/
MUX=1
HYDRAPROC=2
OPROF=0
BENCHID=`date '+%Y%m%d-%H%M%S'`
NUMQ=2000
QUERIES=queries
LOG=`pwd`/tmp/$BENCHID

echo -n "Benchmark name: "
read NAME

echo "Stopping mux"
( cd $SH && su sherlock -c 'bin/mcontrol stop' )

echo "Stopping sherlockd"
( cd $SH && su sherlock -c 'bin/scontrol stop' )

echo "Stopping oprofile"
opcontrol --shutdown

sleep 2

echo "Flushing disk caches (remounting /big)"
umount /big
sync
mount /big

SSO="-SSearch.LogFile=$LOG.slog -SSearch.HydraProcesses=$HYDRAPROC"
echo "Starting sherlockd $SSO"
( cd $SH && su sherlock -c "bin/scontrol start $SSO" )

if [ $MUX != 0 ] ; then
	MO="-SMux.LogFile=$LOG.mlog -SMux.Port=8888"
	echo "Starting mux $MO"
	( cd $SH && su sherlock -c "bin/mcontrol start $MO" )
	PORT=8888
else
	PORT=8192
fi

if [ $OPROF != 0 ] ; then
	echo "Starting oprofile"
	opcontrol --reset
	opcontrol --start
fi

echo "Benchmarking"
echo >$LOG.log -e "\n### $NAME ($BENCHID) ###"
$SH/bin/bench -h localhost:$PORT -n$NUMQ -l$LOG.times <$QUERIES >>$LOG.log
cat $LOG.log

if [ $OPROF != 0 ] ; then
	echo "Stopping oprofile"
	opcontrol --shutdown
fi

if [ $MUX != 0 ] ; then
	echo "Stopping mux"
	( cd $SH && su sherlock -c 'bin/mcontrol stop' )
fi

echo "Stopping sherlockd"
( cd $SH && su sherlock -c 'bin/scontrol stop' )

echo
echo "Analysing search server log"
$SH/bin/log-qsplit <$LOG.slog | $SH/bin/log-ssstats >$LOG.sstats
grep -C2 '^Total:' $LOG.sstats

if [ $MUX != 0 ] ; then
	echo
	echo "Analysing mux log"
	$SH/bin/log-qsplit <$LOG.mlog | $SH/bin/log-muxstats >$LOG.mstats
	grep -E '^(TIME|Avg|Max):' $LOG.mstats
fi

echo "Done."
echo -e '\a'
