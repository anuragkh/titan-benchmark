if [ "$1" = "" ]; then
  echo "Must specify dataset."
  exit
fi

dataset=$1

nodetool upgradesstables -a $dataset edgestore
nodetool upgradesstables -a $dataset graphindex
