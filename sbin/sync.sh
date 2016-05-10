#!/bin/bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

DELETE_FLAG=""

usage() {
  echo "Usage: copy-dir [--delete] <dir>"
  exit 1
}

while :
do
  case $1 in
    --delete)
      DELETE_FLAG="--delete"
      shift
      ;;
    -*)
      echo "ERROR: Unknown option: $1" >&2
      usage
      ;;
    *) # End of options
      break
      ;;
  esac
done

if [[ "$#" != "1" ]] ; then
  usage
fi

if [[ ! -e "$1" ]] ; then
  echo "File or directory $1 doesn't exist!"
  exit 1
fi

DIR=`readlink -f "$1"`
DIR=`echo "$DIR"|sed 's@/$@@'`
DEST=`dirname "$DIR"`

if [ "$hosts" = "" ]; then
  HOSTSLIST=`cat $sbin/../conf/hosts`
else
  HOSTSLIST=`cat $hosts`
fi

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=5"

echo "RSYNC'ing $DIR to hosts..."
for host in $HOSTSLIST; do
    echo $host
    rsync -e "ssh $SSH_OPTS" -az $DELETE_FLAG "$DIR" "$host:$DEST" & sleep 0.5
done
wait
