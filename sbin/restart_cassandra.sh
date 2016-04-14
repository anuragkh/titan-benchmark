#!/usr/bin/env bash
#sudo sh -c 'service cassandra restart'
#sleep 20 # Wait for all connections to establish
sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
nodetool invalidaterowcache
nodetool invalidatekeycache
nodetool invalidatecountercache
nodetool enablethrift
sleep 10
nodetool statusthrift
