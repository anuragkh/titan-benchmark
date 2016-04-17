#!/usr/bin/env bash
set -ex

#### Initial setup

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

dataset=twitter
HOSTLIST=`cat ${sbin}/../conf/hosts`
query_dir=~/${dataset}Queries
results=~/results
mkdir -p $results
OUTPUT_DIR=output

warmup=$((3*60))
measure=$((5*60))
cooldown=$((10*60))

if [ "$dataset" = "twitter" ]; then
    NUM_NODES="41652230"
elif [ "$dataset" = "uk" ]; then
    NUM_NODES="105896555"
else
    echo "Unknown dataset $dataset"
    exit
fi

numClients=( 16 16 )
tests=(
  ## Primitive queries
  Neighbor
  NeighborNode
  EdgeAttr
  NeighborAtype
  NodeNode
  MixPrimitive
  ## TAO queries
  AssocRange
  ObjGet
  AssocGet
  AssocCount
  AssocTimeRange
  MixTao
  MixTaoWithUpdates
)

bash ${sbin}/sync.sh ${sbin}/../

function prepare() {
bash ${sbin}/hosts.sh bash ${sbin}/prepare.sh ${OUTPUT_DIR} ${query_dir}
}

function restart_all() {
  bash ${sbin}/hosts.sh \
    bash ${sbin}/restart_cassandra.sh
}

function timestamp() {
  date +"%D-%T"
}

for clients in ${numClients[*]}; do
    for test in "${tests[@]}"; do
      restart_all
      prepare

      launcherStart=$(date +"%s")
      bash ${sbin}/hosts.sh \
        mvn -f titan-benchmark/pom.xml exec:java -Dexec.mainClass="edu.berkeley.cs.benchmark.Benchmark" \
          -Dexec.args="${test} throughput ${dataset} ${query_dir} ${OUTPUT_DIR} ${clients} ${warmup} ${measure} ${cooldown} ${NUM_NODES}"
      launcherEnd=$(date +"%s")

      bash ${sbin}/hosts.sh \
        tail -n1 ${OUTPUT_DIR}/${test}_throughput.csv | cut -d'	' -f2 >> ${results}/thput
      sum=$(awk '{ sum += $1} END {print sum}' ${results}/thput)
      cat ${results}/thput

      f="${results}/${test}-${clients}clients.txt"
      t=$(timestamp)
      touch ${f}
      echo "$t,$test" >> ${f}
      echo "Measured from master: $((launcherEnd - launcherStart)) secs" >> ${f}
      cat ${results}/thput >> ${f}

      entry="${t}, ${test}, ${clients}, ${sum}"
      echo $entry
      echo $entry >> ${results}/overall_throughput
      rm ${results}/thput
    done
done
