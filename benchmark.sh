#!/bin/bash
set -ex

dataset=orkut
latency=true
throughput=true
QUERY_DIR=~/${dataset}Queries
OUTPUT_DIR=~/${dataset}Output
mkdir -p $OUTPUT_DIR

if [ "$dataset" = "twitter" ]; then
    NUM_NODES="41652230"
elif [ "$dataset" = "uk" ]; then
    NUM_NODES="105896555"
elif [ "$dataset" = "orkut" ]; then
    NUM_NODES="3072627"
else
    echo "Unknown dataset $dataset"
    exit
fi

# List of all possible queries you can benchmark against
# Comment any out if you don't want to benchmark them
tests=(
  # Primitive queries
  Neighbor
  NeighborNode
  EdgeAttr
  NeighborAtype
  NodeNode
  MixPrimitive
  # TAO queries
  AssocRange
  ObjGet
  AssocGet
  AssocCount
  AssocTimeRange
  MixTao
  MixTaoWithUpdates
)

#JVM_HEAP=6900
#echo "Setting -Xmx to ${JVM_HEAP}m"
export MAVEN_OPTS="-verbose:gc -server -Xmx50000M"

warmup=20000
measure=100000
warmup_time=60
measure_time=180
cooldown_time=30
numClients=( 64 )

if [[ "$throughput" = true ]]; then
  for test in "${tests[@]}"; do
    for numClient in "${numClients[@]}"; do
      #sudo sh -c 'service cassandra stop'
      #sleep 5
      sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      #sudo sh -c 'service cassandra start'
      #sleep 15
      nodetool invalidaterowcache
      nodetool invalidatekeycache
      nodetool invalidatecountercache
      #sleep 2
      mvn exec:java -Dexec.mainClass="edu.berkeley.cs.benchmark.Benchmark" \
        -Dexec.args="${test} throughput ${dataset} ${QUERY_DIR} ${OUTPUT_DIR} ${numClient} ${warmup_time} ${measure_time} ${cooldown_time} ${NUM_NODES}"
    done
  done
fi

if [ "$latency" = true ]; then
  for test in "${tests[@]}"; do
    #sudo sh -c 'service cassandra stop'
    #sleep 5
    sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    #sudo sh -c 'service cassandra start'
    #sleep 15
    nodetool invalidaterowcache
    nodetool invalidatekeycache
    nodetool invalidatecountercache
    #sleep 2
    mvn exec:java -Dexec.mainClass="edu.berkeley.cs.benchmark.Benchmark" \
      -Dexec.args="${test} latency ${dataset} ${QUERY_DIR} ${OUTPUT_DIR} ${warmup} ${measure} ${NUM_NODES}"
  done
fi

