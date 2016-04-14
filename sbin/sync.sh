sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

HOSTLIST=`cat ${sbin}/../conf/hosts`

copyDir=$1

if [ "$copyDir" = "" ]; then
  echo "Must specify directory to copy."
  exit
fi

copyDir=$(readlink -e "$copyDir")

for host in `echo "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
  echo "Syncing $copyDir to ${host}..."
  rsync -arL $copyDir ${host}:$copyDir &
  sleep 1
done

wait
