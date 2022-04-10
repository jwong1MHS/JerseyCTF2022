#!/bin/bash
# Sherlock Indexer Script
# (c) 2001--2006 Martin Mares <mj@ucw.cz>
# (c) 2003--2007 Robert Spalek <robert@ucw.cz>
# (c) 2008 Pavel Charvat <pchar@ucw.cz>

function usage
{
	cat >&2 <<EOF
Usage: indexer [-12RUacd:fi:luvwC:S:] [<source> [<dest-dir>]]

-1	Stop after stage 1
-2	Start with stage 2 (needs -d3 at the last time)
-R	Try to resume an interrupted indexation with the same data source
-a	Ignore filters and accept all documents
-c	Only clean files and exit (set -d)
-d NUM	Delete files of level smaller than NUM (default=4)
	9=index, 7=logs, 4=useful/short debug files, 3=labels,
	2=huge files, 1=really temporary files, 0=keep all versions
-f	Force deletion of the old index
-i DIR	Target directory
-l	List index files between passes
-s SRC	Add source
-u	Upload feedback to the gatherer
-U	Do not upload feedback to the gatherer
-v	Be verbose (also enables progress indicators and tracing messages)
-vv	Be more verbose (and so on)
-w	Calculate weights and send gatherer feedback only
-W	Skip calculation of dynamic weights
-C, -S	Global configuration options passed to all programs
EOF
	exit 1
}

unset G LISTS STAGE1ONLY STAGE2ONLY WEIGHTSONLY CLEAN FORCE RESUME FEEDBACK NOWEIGHTS SOURCES[@] TARGET
DELETE=4
VERBOSE=0
set -e
while getopts "12RUWacd:fi:ls:uvwC:S:" OPT ; do
	case "$OPT" in
	        1)	STAGE1ONLY=1
			;;
		2)	STAGE2ONLY=2
			;;
		R)	RESUME=1
			;;
		a)	G="$G -SIndexer.Filter=\"\""
			;;
		c)	CLEAN=1
			;;
		d)	DELETE=$OPTARG
			;;
		f)	FORCE=1
			;;
		i)	TARGET="$OPTARG"
			;;
		l)	LISTS=1
			;;
		s)	SOURCES=("${SOURCES[@]}" "$OPTARG")
			;;
		u)	FEEDBACK=1
			;;
		U)	FEEDBACK=0
			;;
		v)	VERBOSE=$(($VERBOSE+1))
			;;
		w)	WEIGHTSONLY=1
			;;
		W)	NOWEIGHTS=1
			;;
		[CS])	G="$G -$OPT$OPTARG" ;;
		*)	usage
			;;
	esac
done
if [ $VERBOSE -gt 0 ] ; then
	G="$G -SIndexer.ProgressScreen=1 -SIndexer.Trace=$VERBOSE"
fi

# Gather option arguments, so that we can pass them again
OPTION_ARGS=
while [ $OPTIND -gt 1 ] ; do
	OPTION_ARGS="$OPTION_ARGS $1"
	shift
	OPTIND=$(($OPTIND-1))
done

if [ -n "$3" ] ; then
	usage
fi
if [ -n "$1" ] ; then
	SOURCES=("${SOURCES[@]}" "$1")
fi
if [ -n "$2" ] ; then
	TARGET="$2"
fi
if [ -n "$SH_ICONNECT_SRC" ] ; then
	SOURCES=($SH_ICONNECT_SRC)
fi
if [ ${#SOURCES} -ne 0 ] ; then
	G="$G -SIndexer{Source:clear"
	for S in ${SOURCES[*]} ; do
		G="$G;Source=$S"
	done
	G="$G}"
fi
if [ -n "$TARGET" ] ; then
	G="$G -SIndexer.Directory=$TARGET"
	OPTION_ARGS="$OPTION_ARGS -SIndexer.Directory=$TARGET"
fi

function log
{
	bin/logger indexer I "$1"
}

function die
{
	bin/logger indexer ! "$1"
	exit 1
}

function delete
{
	level=$1
	if [ "$level" -lt "$DELETE" ] ; then
		while [ -n "$2" ] ; do
			rm -f "$DIR/$2"
			shift
		done
	fi
}

function deleteall
{
	level=$1
	if [ "$level" -lt "$DELETE" ] ; then
		while [ -n "$2" ] ; do
			rm -f $DIR/$2
			shift
		done
	fi
}

function keep
{
	if [ "$DELETE" -le 0 ] ; then
		suffix=$1
		while [ -n "$2" ] ; do
			cp "$DIR/$2" "$DIR/$2.$suffix"
			shift
		done
	fi
}

function stats
{
	log "Disk usage $1: `du -s "$DIR" | cut -f 1` blocks"
	[ -z "$LISTS" ] || ( ls -Al "$DIR" | bin/logger indexer D )
}

function sizes
{
	bin/sizer $1/{card-attrs,cards,lexicon,references,string-map}
	total_index=`du -bs $1 | cut -f 1`
	bin/logger sizer I "total index size is $total_index"
}

function disconnect
{
	if [ -z "${SRC##* fd:*}" ] ; then
		bin/iconnect --disconnect $G
	fi
}

eval `bin/config $G "Indexer{Directory=not/configured; @Source{String}; LexByFreq; CardPrints; @SubIndex{Name; -#TypeMask; -#IdMask}}"`

SUBINDICES="${CF_Indexer_SubIndex_Name[*]}"
DIR="$CF_Indexer_Directory"
SRC=" ${CF_Indexer_Source_String[*]}"

if [ -n "$RESUME" -a -n "${SRC##* fd:*}" ] ; then
	if ! SRC=" `cat "$DIR/source" 2>/dev/null`"; then
		die "Cannot find $DIR/source"
	fi
fi
#ifdef CONFIG_SHEPHERD_PROTOCOL
if [ -z "${SRC##* remote:*}" ] ; then
	[ -z "$STAGE2ONLY" -o -n "$RESUME" ] || die "When indexing remotely, you cannot use -2 without -R"
	[ -n "$FEEDBACK" -o -n "$WEIGHTSONLY" -o -n "$STAGE2ONLY" ] || die "When indexing remotely, you must choose either -u or -U"
	exec bin/iconnect $SRC - $0$OPTION_ARGS
fi
#endif

if [ "$DELETE" -gt 2 ]; then
	G="$G -SIndexer.SortDeleteSrc=1"
fi

delete 0 attributes.* merges.*
delete 4 lexicon-by-freq
if [ -n "$CLEAN" ]; then
	delete 1 frame-graph image-graph
	delete 2 links-by-url ref-texts url-list-translated lexicon-raw
	delete 2 graph-leaf graph-intra-deg graph-intra-real graph-intra-goes graph-intra-number
	deleteall 2 graph-intra-[0-9]*
	delete 3 labels-by-id labels
	delete 4 merges fingerprints fp-splits checksums signatures keywords url-list
	delete 4 graph-obj graph-obj-index graph-obj-deg graph-obj-real graph-obj-goes
	delete 4 graph-skel graph-skel-index
	delete 4 attributes notes notes-skel
	delete 4 rank-obj rank-skel
	delete 4 card-info
	delete 7 large-classes matches weights lexicon-classes
	if [ -n "$SUBINDICES" ] ; then
		for s in $SUBINDICES ; do
			delete 2 $s/string-index $s/word-index $s/lexicon-ordered $s/lexicon-words $s/stems-ordered
		done
	else
		delete 2 string-index word-index lexicon-ordered lexicon-words stems-ordered
	fi
	exit 0
fi

log "Building index from$SRC in $DIR"
if [ -z "$STAGE2ONLY" ] ; then
	log "Deleting old index"
	mkdir -p "$DIR"
	ls "$DIR"/* >/dev/null 2>&1 \
	&& stty >/dev/null 2>&1 \
	&& if [ "$FORCE" != 1 ]
	then
		echo -n "Delete old index? (y/N) "
		read answ
		if [ "$answ" != y ]; then exit; fi
	fi
	rm -rf "$DIR"/*
fi
for s in $SUBINDICES ; do mkdir -p "$DIR/$s" ; done

#ifdef CONFIG_SHEPHERD_PROTOCOL
if [ -z "${SRC##* fd:*}" ] ; then
	log "Remote data source is $SH_ICONNECT_NORM"
	echo $SH_ICONNECT_NORM >"$DIR/source"
else
#else
if true; then
#endif
	log "Local data source is$SRC"
	echo $SRC >"$DIR/source"
fi

#ifndef CONFIG_BARE
if [ -n "$WEIGHTSONLY" ] ; then
	log "Simplified indexer run for weight calculation"
	bin/scanner $G -SIndexer.{LabelsByID,Checksums,Signatures,RefTexts}=-
	>"$DIR/labels-by-id"
#ifdef CONFIG_SITES
	bin/sitefinder $G
#endif
	bin/fpsort $G
	bin/mkgraph $G
	bin/backlinker $G -1
#ifdef CONFIG_WEIGHTS
	bin/weights $G -R$DELETE
#endif
#ifdef CONFIG_SHEPHERD_PROTOCOL
	[ "$FEEDBACK" == 0 ] || bin/feedback-gath $G
#endif
	log "Weights calculated"
	exit 0
fi
#endif

if [ -z "$STAGE2ONLY" ] ; then
	bin/scanner $G
	keep scanner attributes merges
	[ -z "$STAGE1ONLY" -o "$FEEDBACK" == 1 ] || disconnect
#ifdef CONFIG_SITES
	bin/sitefinder $G
#endif
#ifndef CONFIG_BARE
	bin/fpsort $G
	bin/mkgraph $G
	delete 2 links-by-url
	bin/backlinker $G -1
	keep backlinker1 attributes merges
#endif
#ifdef CONFIG_WEIGHTS
	if [ -z "$NOWEIGHTS" ] ; then
		bin/weights $G -R$DELETE
		keep weights attributes
	fi
#endif
#ifdef CONFIG_BARE
	bin/merger $G
	bin/attrsort $G
	bin/labelsort $G
#else
#ifdef CONFIG_SHEPHERD_PROTOCOL
	[ "$FEEDBACK" == 0 ] || bin/feedback-gath $G
#endif
	bin/keywords $G
	bin/backlinker $G -2
	delete 1 frame-graph image-graph
	keep backlinker2 attributes merges
#ifndef CONFIG_AREAS
	bin/mergefp $G
	keep mergefp merges
#endif
	bin/mergesums $G
	keep mergesums merges
	bin/mergesigns $G
	keep mergesigns merges
#ifdef CONFIG_IMAGES_DUP
	bin/mergeimages $G
	delete 2 image-thumbnails
	keep mergeimages merges
#endif
	delete 4 signatures
	bin/merger $G
	bin/reftexts $G
	delete 2 ref-texts
	bin/attrsort $G
	bin/labelsort $G
	bin/ireport $G
#endif
	stats "after first stage"
	delete 2 url-list-translated
	delete 3 labels-by-id
	delete 4 attributes notes notes-skel
	delete 4 merges fingerprints fp-splits checksums keywords url-list
	delete 4 graph-obj graph-obj-index graph-obj-deg graph-obj-real graph-obj-goes
	delete 4 graph-skel graph-skel-index
	[ "$DELETE" -le 2 ] || stats "after cleanup"
	[ -z "$STAGE1ONLY" ] || exit 0
fi
bin/mklex $G
[ "$DELETE" -gt 4 ] || bin/lexfreq $G
bin/lexorder $G
delete 2 lexicon-raw
bin/chewer $G
disconnect
stats "after chewing"
delete 3 labels
delete 4 card-info
delete 7 large-classes matches weights lexicon-classes

if [ -n "$SUBINDICES" ] ; then
	for s in $SUBINDICES ; do
		log "Processing subindex $s"
		ln -sf ../{lexicon-ordered,stems-ordered} "$DIR/$s/"
		SG="$G -SIndexer.Directory=$DIR/$s"
#ifdef CONFIG_IMAGES_SIM
		bin/imagesigs $SG
		delete 2 $s/image-signatures-unsorted
#endif
		bin/ssort $SG
		delete 2 $s/string-index
		bin/wsort $SG
		delete 2 $s/word-index $s/lexicon-ordered
		bin/lexsort $SG --optimize
		delete 2 $s/lexicon-words $s/stems-ordered
		[ -z "$CF_Indexer_CardPrints" ] || bin/psort $SG
#ifdef CONFIG_SITES
		ln "$DIR/sites" "$DIR/$s/sites"
#endif
		bin/seal $SG
		sizes "$DIR/$s"
	done
	delete 2 lexicon-ordered stems-ordered
else
#ifdef CONFIG_IMAGES_SIM
	bin/imagesigs $G
	delete 2 image-signatures-unsorted
#endif
	bin/ssort $G
	delete 2 string-index
	bin/wsort $G
	delete 2 word-index lexicon-ordered
	bin/lexsort $G
	delete 2 lexicon-words stems-ordered
	[ -z "$CF_Indexer_CardPrints" ] || bin/psort $G
	bin/seal $G
	sizes "$DIR"
fi

stats "after second stage"
log "Index built successfully."
