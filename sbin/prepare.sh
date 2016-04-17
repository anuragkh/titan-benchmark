#!/usr/bin/env bash
set -ex

mkdir -p $1
export MAVEN_OPTS="-server -Xmx50000M"
mvn -f /home/ubuntu/titan-benchmark/pom.xml compile

query_dir=$2

WARMUP_FILES="assocCount_warmup  assocGet_warmup  assocRange_warmup  assocTimeRange_warmup  neighborAtype_warmup_100000  neighbor_node_warmup_100000  neighbor_warmup_100000  objGet_warmup"
QUERY_FILES="assocCount_query  assocGet_query  assocRange_query  assocTimeRange_query  neighborAtype_query_100000  neighbor_node_query_100000  neighbor_query_100000  objGet_query"

for w in $WARMUP_FILES; do
  shuf -n 200000 $query_dir/$w > $query_dir/${w}.txt
done

for q in $QUERY_FILES; do
  shuf -n 1000000 $query_dir/$q > $query_dir/${q}.txt
done
