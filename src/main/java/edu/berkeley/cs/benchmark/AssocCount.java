package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

public class AssocCount extends Benchmark<Long> {
  public static final String WARMUP_FILE = "assocCount_warmup.txt";
  public static final String QUERY_FILE = "assocCount_query.txt";

  @Override public void readQueries() {
    getLongInteger(WARMUP_FILE, warmupAssocCountNodes, warmupAssocCountAtypes);
    getLongInteger(QUERY_FILE, assocCountNodes, assocCountAtypes);
  }

  @Override public Long warmupQuery(Graph g, int i) {
    return g.assocCount(warmupAssocCountNodes[i], warmupAssocCountAtypes[i]);
  }

  @Override public Long query(Graph g, int i) {
    return g.assocCount(assocCountNodes[i], assocCountAtypes[i]);
  }

  @Override public RunThroughput getThroughputJob(int clientId) {
    return new RunThroughput(clientId) {
      @Override public void warmupQuery() {
        int idx = rand.nextInt(assocCount_warmup);
        AssocCount.this.warmupQuery(g, idx);
      }

      @Override public int query() {
        int idx = rand.nextInt(assocCount_query);
        return AssocCount.this.query(g, idx).intValue();
      }
    };
  }

}
