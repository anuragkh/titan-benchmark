#!/usr/bin/env bash
set -ex

mkdir -p $1
export MAVEN_OPTS="-server -Xmx50000M"
mvn -f /home/ubuntu/titan-benchmark/pom.xml compile

query_dir=$2

shuf -n 2000000 $query_dir/assocCount_warmup > $query_dir/assocCount_warmup.txt
shuf -n 10000000 $query_dir/assocCount_query > $query_dir/assocCount_query.txt

shuf -n 200000 $query_dir/assocGet_warmup > $query_dir/assocGet_warmup.txt
shuf -n 1000000 $query_dir/assocGet_query > $query_dir/assocGet_query.txt

shuf -n 2000000 $query_dir/assocRange_warmup > $query_dir/assocRange_warmup.txt
shuf -n 10000000 $query_dir/assocRange_query > $query_dir/assocRange_query.txt

shuf -n 2000000 $query_dir/assocTimeRange_warmup > $query_dir/assocTimeRange_warmup.txt
shuf -n 10000000 $query_dir/assocTimeRange_query > $query_dir/assocTimeRange_query.txt

shuf -n 2000000 $query_dir/objGet_warmup > $query_dir/objGet_warmup.txt
shuf -n 10000000 $query_dir/objGet_query > $query_dir/objGet_query.txt

shuf -n 2000000 $query_dir/neighborAtype_warmup_100000 > $query_dir/neighborAtype_warmup_100000.txt
shuf -n 10000000 $query_dir/neighborAtype_query_100000 > $query_dir/neighborAtype_query_100000.txt

shuf -n 1000000 $query_dir/neighbor_node_warmup_100000 > $query_dir/neighbor_node_warmup_100000.txt
shuf -n 6000000 $query_dir/neighbor_node_query_100000 > $query_dir/neighbor_node_query_100000.txt

shuf -n 1000000 $query_dir/neighbor_warmup_100000 > $query_dir/neighbor_warmup_100000.txt
shuf -n 6000000 $query_dir/neighbor_query_100000 > $query_dir/neighbor_query_100000.txt

shuf -n 1000000 $query_dir/node_query_100000 > $query_dir/node_query_100000.txt
shuf -n 6000000 $query_dir/node_warmup_100000 > $query_dir/node_warmup_100000.txt