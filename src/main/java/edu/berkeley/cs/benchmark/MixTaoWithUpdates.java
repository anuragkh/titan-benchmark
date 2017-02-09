package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Assoc;
import edu.berkeley.cs.titan.Graph;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

public class MixTaoWithUpdates extends Benchmark<Object> {
  // Read workload distribution; from ATC 13 Bronson et al.
  final static double ASSOC_RANGE_PERC = 0.409;
  final static double OBJ_GET_PERC = 0.289;
  final static double ASSOC_GET_PERC = 0.157;
  final static double ASSOC_COUNT_PERC = 0.117;
  final static double ASSOC_TIME_RANGE_PERC = 0.028;
  final static double TAO_WRITE_PERC = 0.002;
  final static double ASSOC_ADD_PERC = 0.76;
  final static double OBJ_ADD_PERC = 0.24;

  public static int chooseQuery(Random randUpdate, Random rand) {
    double u = randUpdate.nextDouble();
    double d = rand.nextDouble();
    if (u < TAO_WRITE_PERC) {
      if (d < ASSOC_ADD_PERC) {
        return 5;
      } else {
        return 6;
      }
    }

    if (d < ASSOC_RANGE_PERC) {
      return 0;
    } else if (d < ASSOC_RANGE_PERC + OBJ_GET_PERC) {
      return 1;
    } else if (d < ASSOC_RANGE_PERC + OBJ_GET_PERC + ASSOC_GET_PERC) {
      return 2;
    } else if (d < ASSOC_RANGE_PERC + OBJ_GET_PERC +
      ASSOC_GET_PERC + ASSOC_COUNT_PERC) {
      return 3;
    } else {
      return 4;
    }
  }

  @Override public void readQueries() {
    // assoc_range()
    AssocRange
      .readAssocRangeQueries(AssocRange.WARMUP_FILE, warmupAssocRangeNodes, warmupAssocRangeAtypes,
        warmupAssocRangeOffsets, warmupAssocRangeLengths);
    AssocRange.readAssocRangeQueries(AssocRange.QUERY_FILE, assocRangeNodes, assocRangeAtypes,
      assocRangeOffsets, assocRangeLengths);

    // assoc_count()
    getLongInteger(AssocCount.WARMUP_FILE, warmupAssocCountNodes, warmupAssocCountAtypes);
    getLongInteger(AssocCount.QUERY_FILE, assocCountNodes, assocCountAtypes);

    // obj_get()
    getLong(ObjGet.WARMUP_FILE, warmupObjGetIds);
    getLong(ObjGet.QUERY_FILE, objGetIds);

    // assoc_get()
    AssocGet.readAssocGetQueries(AssocGet.WARMUP_FILE, warmupAssocGetNodes, warmupAssocGetAtypes,
      warmupAssocGetDstIdSets, warmupAssocGetTimeLows, warmupAssocGetTimeHighs);

    AssocGet
      .readAssocGetQueries(AssocGet.QUERY_FILE, assocGetNodes, assocGetAtypes, assocGetDstIdSets,
        assocGetTimeLows, assocGetTimeHighs);

    // assoc_time_range()
    AssocTimeRange.readAssocTimeRangeQueries(AssocTimeRange.WARMUP_FILE, warmupAssocTimeRangeNodes,
      warmupAssocTimeRangeAtypes, warmupAssocTimeRangeTimeLows, warmupAssocTimeRangeTimeHighs,
      warmupAssocTimeRangeLimits);

    AssocTimeRange.readAssocTimeRangeQueries(AssocTimeRange.QUERY_FILE, assocTimeRangeNodes,
      assocTimeRangeAtypes, assocTimeRangeTimeLows, assocTimeRangeTimeHighs, assocTimeRangeLimits);
  }

  @Override public void benchLatency() {
    Graph graph = new Graph();
    PrintWriter assocRangeOut = makeFileWriter("mix_AssocRange.csv", false);
    PrintWriter objGetOut = makeFileWriter("mix_ObjGet.csv", false);
    PrintWriter assocGetOut = makeFileWriter("mix_AssocGet.csv", false);
    PrintWriter assocCountOut = makeFileWriter("mix_AssocCount.csv", false);
    PrintWriter assocTimeRangeOut = makeFileWriter("mix_AssocTimeRange.csv", false);
    PrintWriter assocAddOut = makeFileWriter("mix_AssocAdd.csv", false);
    PrintWriter objAddOut = makeFileWriter("mix_ObjAdd.csv", false);

    Random rand = new Random(SEED);
    Random randUpdate = new Random(SEED);

    long id1, id2;
    int atype;

    System.out.println("Titan MixPrimitive tao query latency");
    System.out.println("Warming up for " + WARMUP_N + " queries");
    for (int i = 0; i < WARMUP_N; i++) {
      if (i % 10000 == 0) {
        graph.restartTransaction();
        System.out.println("Warmed up for " + i + " queries");
      }

      switch (chooseQuery(randUpdate, rand)) {
        case 0:
          // assoc_range
          graph.assocRange(warmupAssocRangeNodes[i], warmupAssocRangeAtypes[i],
            warmupAssocRangeOffsets[i], warmupAssocRangeLengths[i]).size();
          break;
        case 1:
          // obj_get
          graph.objGet(warmupObjGetIds[i]).size();
          break;
        case 2:
          // assoc_get
          graph.assocGet(warmupAssocGetNodes[i], warmupAssocGetAtypes[i],
            //                            warmupAssocGetDstIdSets[i],
            new HashSet(Arrays.asList(warmupAssocGetDstIdSets[i])), warmupAssocGetTimeLows[i],
            warmupAssocGetTimeHighs[i]).size();
          break;
        case 3:
          // assoc_count
          graph.assocCount(warmupAssocCountNodes[i], warmupAssocCountAtypes[i]);
          break;
        case 4:
          // assoc_time_range
          graph.assocTimeRange(warmupAssocTimeRangeNodes[i], warmupAssocTimeRangeAtypes[i],
            warmupAssocTimeRangeTimeLows[i], warmupAssocTimeRangeTimeHighs[i],
            warmupAssocTimeRangeLimits[i]).size();
          break;
        case 5:
          // assoc_add
          id1 = Math.abs(rand.nextLong()) % NUM_NODES;
          id2 = Math.abs(rand.nextLong()) % NUM_NODES;
          atype = rand.nextInt(5);
          graph.assocAdd(id1, id2, atype, System.currentTimeMillis(), EDGE_ATTR);
          break;
        case 6:
          // obj_add
          graph.objAdd(Integer.MAX_VALUE + (long) rand.nextInt(Integer.MAX_VALUE), NODE_ATTRS);
          break;
      }
    }

    rand.setSeed(SEED + 1); // re-seed
    randUpdate.setSeed(SEED + 1);
    long start, end;

    System.out.println("Measuring for " + MEASURE_N + " queries");
    for (int i = 0; i < MEASURE_N; i++) {
      if (i % 10000 == 0) {
        graph.restartTransaction();
        System.out.println("Measured for " + i + " queries");
      }
      List<Assoc> assocs;
      switch (chooseQuery(randUpdate, rand)) {
        case 0:
          // assoc_range
          start = System.nanoTime();
          assocs = graph.assocRange(assocRangeNodes[i], assocRangeAtypes[i], assocRangeOffsets[i],
            assocRangeLengths[i]);
          end = System.nanoTime();
          assocRangeOut.println(assocs.size() + "," + (end - start) / 1e3);
          break;
        case 1:
          // obj_get
          start = System.nanoTime();
          List<String> attrs = graph.objGet(objGetIds[i]);
          end = System.nanoTime();
          objGetOut.println(attrs.size() + "," + (end - start) / 1e3);
          break;
        case 2:
          // assoc_get
          start = System.nanoTime();
          assocs = graph.assocGet(assocGetNodes[i], assocGetAtypes[i],
            new HashSet(Arrays.asList(assocGetDstIdSets[i])),
            //                            assocGetDstIdSets[i],
            assocGetTimeLows[i], assocGetTimeHighs[i]);
          end = System.nanoTime();
          assocGetOut.println(assocs.size() + "," + (end - start) / 1e3);
          break;
        case 3:
          // assoc_count
          start = System.nanoTime();
          long count = graph.assocCount(assocCountNodes[i], assocCountAtypes[i]);
          end = System.nanoTime();
          assocCountOut.println(count + "," + (end - start) / 1e3);
          break;
        case 4:
          // assoc_time_range
          start = System.nanoTime();
          assocs = graph.assocTimeRange(assocTimeRangeNodes[i], assocTimeRangeAtypes[i],
            assocTimeRangeTimeLows[i], assocTimeRangeTimeHighs[i], assocTimeRangeLimits[i]);
          end = System.nanoTime();
          assocTimeRangeOut.println(assocs.size() + "," + (end - start) / 1e3);
          break;
        case 5:
          // assoc_add
          start = System.nanoTime();
          id1 = Math.abs(rand.nextLong()) % NUM_NODES;
          id2 = Math.abs(rand.nextLong()) % NUM_NODES;
          atype = rand.nextInt(5);
          graph.assocAdd(id1, id2, atype, System.currentTimeMillis(), EDGE_ATTR);
          end = System.nanoTime();
          assocAddOut.println("1," + (end - start) / 1e3);
          break;
        case 6:
          // obj_add
          start = System.nanoTime();
          graph.objAdd(Integer.MAX_VALUE + (long) rand.nextInt(Integer.MAX_VALUE), NODE_ATTRS);
          end = System.nanoTime();
          objAddOut.println("1," + (end - start) / 1e3);
          break;
      }
    }

    assocRangeOut.close();
    objGetOut.close();
    assocGetOut.close();
    assocCountOut.close();
    assocTimeRangeOut.close();
    assocAddOut.close();
    objAddOut.close();
    Benchmark.printMemoryFootprint();
  }

  @Override public RunThroughput getThroughputJob(int clientId) {
    return new RunThroughput(clientId) {
      Random randUpdate = new Random(SEED);

      @Override public void warmupQuery() {
        int i, atype;
        long id1, id2;
        switch (chooseQuery(randUpdate, rand)) {
          case 0:
            // assoc_range
            i = rand.nextInt(assocRange_warmup);
            g.assocRange(warmupAssocRangeNodes[i], warmupAssocRangeAtypes[i],
              warmupAssocRangeOffsets[i], warmupAssocRangeLengths[i]).size();
            break;
          case 1:
            // obj_get
            i = rand.nextInt(objGet_warmup);
            g.objGet(warmupObjGetIds[i]).size();
            break;
          case 2:
            // assoc_get
            i = rand.nextInt(assocGet_warmup);
            g.assocGet(warmupAssocGetNodes[i], warmupAssocGetAtypes[i],
              //                                warmupAssocGetDstIdSets[i],
              new HashSet(Arrays.asList(warmupAssocGetDstIdSets[i])), warmupAssocGetTimeLows[i],
              warmupAssocGetTimeHighs[i]).size();
            break;
          case 3:
            // assoc_count
            i = rand.nextInt(assocCount_warmup);
            g.assocCount(warmupAssocCountNodes[i], warmupAssocCountAtypes[i]);
            break;
          case 4:
            // assoc_time_range
            i = rand.nextInt(assocTimeRange_warmup);
            g.assocTimeRange(warmupAssocTimeRangeNodes[i], warmupAssocTimeRangeAtypes[i],
              warmupAssocTimeRangeTimeLows[i], warmupAssocTimeRangeTimeHighs[i],
              warmupAssocTimeRangeLimits[i]).size();
            break;
          case 5:
            // assoc_add
            id1 = Math.abs(rand.nextLong()) % NUM_NODES;
            id2 = Math.abs(rand.nextLong()) % NUM_NODES;
            atype = rand.nextInt(5);
            g.assocAdd(id1, id2, atype, System.currentTimeMillis(), EDGE_ATTR);
            break;
          case 6:
            // obj_add
            g.objAdd(Integer.MAX_VALUE + (long) rand.nextInt(Integer.MAX_VALUE), NODE_ATTRS);
            break;

        }
      }

      @Override public int query() {
        int i, atype;
        long id1, id2;
        switch (chooseQuery(randUpdate, rand)) {
          case 0:
            // assoc_range
            i = rand.nextInt(assocRange_query);
            return g.assocRange(assocRangeNodes[i], assocRangeAtypes[i], assocRangeOffsets[i],
              assocRangeLengths[i]).size();
          case 1:
            // obj_get
            i = rand.nextInt(objGet_query);
            return g.objGet(objGetIds[i]).size();
          case 2:
            // assoc_get
            i = rand.nextInt(assocGet_query);
            return g.assocGet(assocGetNodes[i], assocGetAtypes[i],
              //                                assocGetDstIdSets[i],
              new HashSet(Arrays.asList(assocGetDstIdSets[i])), assocGetTimeLows[i],
              assocGetTimeHighs[i]).size();
          case 3:
            // assoc_count
            i = rand.nextInt(assocCount_query);
            g.assocCount(assocCountNodes[i], assocCountAtypes[i]);
            return 1;
          case 4:
            // assoc_time_range
            i = rand.nextInt(assocTimeRange_query);
            return g.assocTimeRange(assocTimeRangeNodes[i], assocTimeRangeAtypes[i],
              assocTimeRangeTimeLows[i], assocTimeRangeTimeHighs[i], assocTimeRangeLimits[i])
              .size();
          case 5:
            // assoc_add
            id1 = Math.abs(rand.nextLong()) % NUM_NODES;
            id2 = Math.abs(rand.nextLong()) % NUM_NODES;
            atype = rand.nextInt(5);
            g.assocAdd(id1, id2, atype, System.currentTimeMillis(), EDGE_ATTR);
            return 1;
          case 6:
            // obj_add
            g.objAdd(Integer.MAX_VALUE + (long) rand.nextInt(Integer.MAX_VALUE), NODE_ATTRS);
            return 1;
        }
        return 0;
      }
    };
  }

  /**
   * These queries are not being used, since benchLatency is overriden.
   */
  @Override public Object warmupQuery(Graph g, int i) {
    return null;
  }

  @Override public Object query(Graph g, int i) {
    return null;
  }

}
