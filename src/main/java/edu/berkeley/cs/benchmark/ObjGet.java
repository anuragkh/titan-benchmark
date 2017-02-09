package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

import java.util.List;

public class ObjGet extends Benchmark<List<String>> {
  public static final String WARMUP_FILE = "objGet_warmup.txt";
  public static final String QUERY_FILE = "objGet_query.txt";

  @Override public void readQueries() {
    getLong(WARMUP_FILE, warmupObjGetIds);
    getLong(QUERY_FILE, objGetIds);
  }

  @Override public List<String> warmupQuery(Graph g, int i) {
    return g.objGet(warmupObjGetIds[i]);
  }

  @Override public List<String> query(Graph g, int i) {
    return g.objGet(objGetIds[i]);
  }

  @Override public RunThroughput getThroughputJob(int clientId) {
    return new RunThroughput(clientId) {
      @Override public void warmupQuery() {
        int idx = rand.nextInt(objGet_warmup);
        ObjGet.this.warmupQuery(g, idx);
      }

      @Override public int query() {
        int idx = rand.nextInt(objGet_query);
        return ObjGet.this.query(g, idx).size();
      }
    };
  }

}
