#!/bin/bash
# Watson install script
#
# (c) 2005 Vladimir Jelen <vladimir.jelen@netcentrum.cz>

SHERLOCK_DIR=`pwd`

if [ ! -f cf/watson ] ; then
	echo >&2 "Missing config file cf/watson!"
	exit 1
fi
. cf/watson

if [ -z "$WATSON_DIR" ] ; then
	echo >&2 "WATSON_DIR is not set in cf/watson!"
	exit 1
fi

if [ -z "$WATSON_USER" ] ; then
	echo >&2 "WATSON_USER is not set in cf/watson!"
	exit 1
fi

if [ -z "$SSH_KEY" ] ; then
	echo >&2 "SSH_KEY is not set in cf/watson"
	exit 1
fi

if [ ! -f "$SSH_KEY" ] ; then
	echo >&2 "Missing ssh private key $SSH_KEY!"
	exit 1
fi

ssh -i "$SSH_KEY" "$WATSON_USER@localhost" `cat <<EOF
mkdir -p "$WATSON_DIR" &&
cd "$WATSON_DIR" &&
if [ -f drwatson.cgi ] ; then mv -f drwatson.cgi drwatson.old.cgi ; fi &&
if [ -f graph.cgi ] ; then mv -f graph.cgi graph.old.cgi ; fi &&
rm -rf graph cache cf &&
rm -f lib log &&
ln -s "$SHERLOCK_DIR/lib" lib &&
ln -s "$SHERLOCK_DIR/log" log &&
cp -a lib/cgi-bin/* . &&
mkdir -p cache/graph cf &&
chmod -R a+w cache &&
cp "$SHERLOCK_DIR/cf/watson" cf &&
rm -f .htaccess &&
echo "DirectoryIndex drwatson.cgi" > .htaccess
EOF`

if [ $? == 0 ] ; then
	echo "Watson installed."
else
	echo "Watson installation FAILED! Please fix manually."
fi
