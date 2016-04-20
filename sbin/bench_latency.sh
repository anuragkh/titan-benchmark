#!/usr/bin/env bash
set -ex

#### Initial setup

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

dataset=twitter
server=`tail -n 1 ${sbin}/../conf/hosts`
query_dir=~/${dataset}Queries
results=~/${dataset}Results
mkdir -p $results
OUTPUT_DIR=output

warmup=200000
measure=1000000

if [ "$dataset" = "twitter" ]; then
    NUM_NODES="41652230"
elif [ "$dataset" = "uk" ]; then
    NUM_NODES="105896555"
else
    echo "Unknown dataset $dataset"
    exit
fi

tests=(
  ## Primitive queries
  Neighbor
  NeighborNode
  EdgeAttr
  NeighborAtype
  NodeNode
  ## TAO queries
  AssocRange
  ObjGet
  AssocGet
  AssocCount
  AssocTimeRange
)

bash ${sbin}/sync.sh ${sbin}/../

bash ${sbin}/hosts.sh source ${sbin}/prepare.sh ${OUTPUT_DIR} ${query_dir}

function restart_all() {
  bash ${sbin}/hosts.sh bash ${sbin}/restart_cassandra.sh
}

function timestamp() {
  date +"%D-%T"
}

for test in "${tests[@]}"; do
  restart_all

  launcherStart=$(date +"%s")
  ssh $server "mvn -f titan-benchmark/pom.xml exec:java -Dexec.mainClass=\"edu.berkeley.cs.benchmark.Benchmark\" -Dexec.args=\"${test} latency ${dataset} ${query_dir} ${OUTPUT_DIR} ${warmup} ${measure} ${NUM_NODES}\""
  launcherEnd=$(date +"%s")

  echo "Benchmark took $((launcherEnd - launcherStart)) secs"
done
