#!/usr/bin/env bash
set -ex

#### Initial setup

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

dataset=twitter
HOSTLIST=`cat ${sbin}/../conf/hosts`
query_dir=~/queries
results=~/results

OUTPUT_DIR=output
warmup=$((3*60))
measure=$((15*60))
cooldown=$((3*60))

numClients=(16 64)
tests=(
  # Primitive queries
  # Neighbor
  # NeighborNode
  # EdgeAttr
  # NeighborAtype
  # NodeNode
  MixPrimitive
  # TAO queries
  # AssocRange
  # ObjGet
  # AssocGet
  # AssocCount
  # AssocTimeRange
  MixTao
)

#### Copy the repo files over
for host in `echo "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
  #rsync -arL ${sbin}/../ ubuntu@${host}:titan-benchmark &
  rsync -arL ${sbin}/vol0/titan/queries ubuntu@${host}:~ &
done
wait
echo "Synced benchmark repo and queries to all servers."

bash ${sbin}/hosts.sh \
  source ${sbin}/prepare.sh ${OUTPUT_DIR}

function restart_all() {
  bash ${sbin}/hosts.sh \
    bash ${sbin}/restart_cassandra.sh
  sleep 60
}

function timestamp() {
  date +"%D-%T"
}

for clients in ${numClients[*]}; do
    for test in "${tests[@]}"; do
      restart_all
      bash ${sbin}/hosts.sh \
        mvn -f titan-benchmark/pom.xml exec:java -Dexec.mainClass="edu.berkeley.cs.benchmark.Benchmark" \
          -Dexec.args="${test} throughput ${dataset} ${query_dir} ${OUTPUT_DIR} ${clients} ${warmup} ${measure} ${cooldown}"

      bash ${sbin}/hosts.sh \
        tail -n1 ${OUTPUT_DIR}/${test}_throughput.csv | cut -d'	' -f2 >> ${results}/thput
      sum=$(awk '{ sum += $1} END {print sum}' ~/results/thput)
      cat ${results}/thput

      f="${results}/${test}-${clients}clients.txt"
      t=$(timestamp)
      touch ${f}
      echo "$t,$test" >> ${f}
      cat ${results}/thput >> ${f}

      entry="${t}, ${test}, ${clients}, ${sum}"
      echo $entry
      echo $entry >> ${results}/overall_throughput
      rm ${results}/thput
    done
done

