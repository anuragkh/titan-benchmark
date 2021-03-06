#!/usr/bin/env bash
# Run a shell command on all hosts.

usage="Usage: hosts.sh command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

sbin="`dirname "$0"`"

if [ "$hosts" = "" ]; then
  HOSTLIST=`cat ${sbin}/../conf/hosts`
else
  HOSTLIST=`cat $hosts`
fi

# By default disable strict host key checking
if [ "$TITAN_SSH_OPTS" = "" ]; then
  TITAN_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

for host in `echo "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
  ssh $TITAN_SSH_OPTS "$host" $"${@// /\\ }" \
    2>&1 | sed "s/^/$host: /" &
done

wait
