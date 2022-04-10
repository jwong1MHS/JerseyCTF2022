#!/bin/bash
# Watson -- an elementary monitoring system for Sherlock Holmes
#
# (c) 2003-2005 Tomas Valla <tom@ucw.cz>
# (c) 2005 Vladimir Jelen <vladimir.jelen@netcentrum.cz>
# (c) 2007-2009 Pavel Charvat <pchar@ucw.cz>

if [ ! -f cf/watson ] ; then
	echo "Missing config file cf/watson !" >&2
	exit 1
fi

DEFAULT_PATH=run/
DEFAULT_LOG_PATH=log/
SEARCH_PREFIX=sherlockd-
REAP_PREFIX=reapd-
GATHERD_PREFIX=gather-
SHEPHERD_PREFIX=shepherd-
INDEXER_PREFIX=indexer-
LEX_PREFIX=lex-
LEX_LOG_SUBDIR=words/
MUX_PREFIX=mux-
SPRAY_PREFIX=spray-
SPROXY_PREFIX=sproxy-
ERROR_MAIL=

. cf/watson

function getval {       # getval <server> <var> <config_prefix> <config_default>
	x=$1 ; eval $2='"${'$3_$x'}"'
	while [ "$x" != "${x%_*}" ] ; do
		x="${x%_*}" ; eval ': "${'$2':=$'$3_$x'}"'
	done
	x=${4:-$3}
	eval ': "${'$2':=$'$x'}"'
	# For <config_prefix>=conf <config_default>=def <server>=abc_def_ghi returns first nonempty string from sequence
	# "$conf_abc_def_ghi", "$conf_abc_def", "$conf_abc", "$def"
}

function download () {	# download <server_type>
	stype="$1"
	eval "servers=\$${stype}_SERVERS"
	for server in $servers ; do
		getval $server prefix ${stype}_PREFIX
		getval $server delete ROTATE_DELETE_${stype}
		getval $server compress ROTATE_COMPRESS_${stype}
		getval $server backup ROTATE_BACKUP_${stype}
		getval $server path PATH DEFAULT_PATH
		getval $server log_path LOG_PATH DEFAULT_LOG_PATH
		getval $server log_subdir ${stype}_LOG_SUBDIR
		getval $server host_expr HOST_EXPR DEFAULT_HOST_EXPR
		host_eval="`eval echo -n $server "$host_expr"`"
		getval $server host HOST host_eval

		if [ -z "$backup" ] ; then
			echo "Missing values in watson config file" 1>&2
		elif [ "$host" != "$host$path$log_path$log_subdir$prefix*" ] ; then
			echo "Fetching $host:$path$log_path$log_subdir$prefix*"
			mkdir -p log/$server log/inter/$server && \
				rsync -vuat -e "ssh -i $SSH_KEY" "$host:$path$log_path$log_subdir$prefix*" log/$server/ && \
				ssh -i "$SSH_KEY" "$host" "${path}bin/rotate-log" "$delete" "$delete" "$path$log_path$log_subdir$prefix*" && \
				bin/rotate-log "$compress" "$backup" log/$server/$prefix*
		fi
	done
}

function check () {	# check <server_type> <analyse_command> <result_prefix>
	stype="$1"
	analyze="$2"
	rprefix="$3"
	eval "servers=\$${stype}_SERVERS"
	for server in $servers ; do
		getval $server prefix ${stype}_PREFIX
		for src in log/$server/$prefix* ; do
			if [ -f "$src" ] ; then
				dest="log/inter/$server/$rprefix`echo -n "$src" | sed 's:^log/[^/]*/::' | sed 's/\.[bg]z2\?//'`"
				if [ ! \( -e "$dest" \) -o \( "$src" -nt "$dest" \) ]; then
					echo "Processing $src into $dest"
					"$analyze" "$src" "$dest"
				fi
			fi
		done
	done
}

# Rotate and download logs
echo "Watson: Rotating and downloading logs"
download SEARCH
download INDEXER
download LEX
#ifdef CONFIG_SHEPHERD_PROTOCOL
download REAP
download SHEPHERD
#endif
download GATHERD

# Analyse logs
echo "Watson: Analyzing logs"
check SEARCH bin/analyze-search
check INDEXER bin/analyze-indexer
check INDEXER bin/analyze-indexer-status status-
#ifdef CONFIG_SHEPHERD_PROTOCOL
check SHEPHERD bin/analyze-shepherd
check SHEPHERD_SERVERS bin/analyze-shepherd-status status-
#endif
check GATHERD bin/analyze-gatherd

# Report errors
if [ -n "$ERROR_MAIL" ] ; then
  echo "Watson: Analyzing gatherer and mux errors"
  ERROR_REPORT=`bin/log-gatherer-bugs`
  if [ -n "$ERROR_REPORT" ] ; then
    echo "$ERROR_REPORT"
    echo "$ERROR_REPORT" | bin/send-mail -s "Gatherer & Mux errors" $ERROR_MAIL
  fi
fi

echo "Watson: Done."
