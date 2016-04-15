package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

import java.util.Random;

public class AssocAdd extends Benchmark<Integer> {
    private Random warmupRand;
    private Random rand;

    @Override
    public void readQueries() {
        warmupRand = new Random(SEED);
        rand = new Random(SEED + 1);
    }

    @Override
    public Integer warmupQuery(Graph g, int i) {
        long id1 = Math.abs(warmupRand.nextLong()) % NUM_NODES;
        long id2 = Math.abs(warmupRand.nextLong()) % NUM_NODES;
        int atype = warmupRand.nextInt(5);
        g.assocAdd(id1, id2, atype, System.currentTimeMillis(), EDGE_ATTR);
        return 0;
    }

    @Override
    public Integer query(Graph g, int i) {
        long id1 = Math.abs(rand.nextLong()) % NUM_NODES;
        long id2 = Math.abs(rand.nextLong()) % NUM_NODES;
        int atype = rand.nextInt(5);
        g.assocAdd(id1, id2, atype, System.currentTimeMillis(), EDGE_ATTR);
        return 0;
    }
}
