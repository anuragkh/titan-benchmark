package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

import java.util.Random;

public class ObjAdd extends Benchmark<Long> {
  private Random warmupRand;
  private Random rand;

  @Override public void readQueries() {
    warmupRand = new Random(SEED);
    rand = new Random(SEED + 1);
  }

  @Override public Long warmupQuery(Graph g, int i) {
    long nodeId = Integer.MAX_VALUE + (long) warmupRand.nextInt(Integer.MAX_VALUE);
    g.objAdd(nodeId, NODE_ATTRS);
    return 0L;
  }

  @Override public Long query(Graph g, int i) {
    long nodeId = Integer.MAX_VALUE + (long) rand.nextInt(Integer.MAX_VALUE);
    g.objAdd(nodeId, NODE_ATTRS);
    return 0L;
  }
}
