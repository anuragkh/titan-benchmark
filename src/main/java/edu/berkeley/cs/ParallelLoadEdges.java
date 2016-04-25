package edu.berkeley.cs;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.util.TitanId;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParallelLoadEdges {

    private static int offset;
    private static String[] edgeFiles;

    public static void main(String[] args) throws ConfigurationException, IOException {
        Configuration config = new PropertiesConfiguration(
                ParallelLoadEdges.class.getResource("/benchmark.properties"));

        offset = config.getBoolean("zero_indexed") ? 1 : 0;
        edgeFiles = args;

        Configuration titanConfiguration = new PropertiesConfiguration(
                ParallelLoadEdges.class.getResource("/titan-cassandra.properties"));

        load(config, titanConfiguration);

        System.exit(0);
    }

    static class EdgeLoader implements Runnable {

        TitanGraph g;
        String edgeFile;

        public EdgeLoader(TitanGraph g, String edgeFile) {
            this.g = g;
            this.edgeFile = edgeFile;
        }

        @Override public void run() {
            try {
                loadEdges(g, edgeFile);
            } catch (IOException e) {
                System.err.println("EdgeLoader thread failed: ");
                e.printStackTrace();
            }

        }
    }

    public static void load(Configuration config, Configuration titanConfig) throws IOException {
        titanConfig.setProperty("storage.batch-loading", true);
        titanConfig.setProperty("schema.default", "none");
        titanConfig.setProperty("graph.set-vertex-id", true);
        titanConfig.setProperty("storage.cassandra.keyspace", config.getString("name"));
        titanConfig.setProperty("storage.batch-loading", true);
        titanConfig.setProperty("storage.buffer-size", 5120);
        titanConfig.setProperty("schema.default", "none");
        titanConfig.setProperty("ids.block-size", 100000);
        titanConfig.setProperty("ids.authority.wait-time", 60000);

        TitanGraph g = TitanFactory.open(titanConfig);
        createSchemaIfNotExists(g, config);
        if (!g.getVertices().iterator().hasNext()) {
            System.err.println("[FATAL] Graph does not have any vertices.");
        } else {
            List<Thread> threads = new ArrayList<>(edgeFiles.length);
            for (String edgeFile : edgeFiles) {
                threads.add(new Thread(new EdgeLoader(g, edgeFile)));
            }

            for (Thread thread: threads) {
                thread.start();
            }

            try {
                for (Thread thread: threads) {
                    thread.join();
                }
            } catch (InterruptedException e) {
                System.err.println("EdgeLoader thread failed: ");
                e.printStackTrace();
            }
        }
    }

    private static void createSchemaIfNotExists(TitanGraph g, Configuration config) {
        TitanManagement mgmt = g.getManagementSystem();
        if (mgmt.containsEdgeLabel("0"))
            return;

        int NUM_ATTR = config.getInt("property.total");
        int NUM_ATYPES = config.getInt("atype.total");

        PropertyKey[] nodeProperties = new PropertyKey[NUM_ATTR];
        for (int i = 0; i < NUM_ATTR; i++) {
            nodeProperties[i] = mgmt.makePropertyKey("attr" + i).dataType(String.class).make();
            mgmt.buildIndex("byAttr" + i, Vertex.class).addKey(nodeProperties[i]).buildCompositeIndex();
        }

        PropertyKey timestamp = mgmt.makePropertyKey("timestamp").dataType(Long.class).make();
        PropertyKey edgeProperty = mgmt.makePropertyKey("property").dataType(String.class).make();
        for (int i = 0; i < NUM_ATYPES; i++) {
            EdgeLabel label = mgmt.makeEdgeLabel(""+ i).signature(timestamp, edgeProperty).unidirected().make();
            if (config.getBoolean("index_timestamp")) {
                mgmt.buildEdgeIndex(label, "byEdge"+i, Direction.OUT, Order.DESC, timestamp);
            }
        }

        mgmt.commit();
    }

    private static void loadEdges(TitanGraph g, String edgeFile) throws IOException  {
        System.out.printf("Loading edgeFile %s...\n", edgeFile);

        long c = 1L;
        long startTime = System.currentTimeMillis();
        try (BufferedReader br = new BufferedReader(new FileReader(edgeFile))) {
            for (String line; (line = br.readLine()) != null; ) {
                List<String> tokens = Lists.newArrayList(Splitter.on(' ').limit(4).split(line));
                Long id1 = Long.parseLong(tokens.get(0)) + offset;
                Long id2 = Long.parseLong(tokens.get(1)) + offset;

                String atype = tokens.get(2);
                String tsAndProp = tokens.get(3);
                int splitIdx = tsAndProp.indexOf(' ');

                Long timestamp = Long.parseLong(tsAndProp.substring(0, splitIdx));
                String property = tsAndProp.substring(splitIdx + 1);

                Vertex v1 = g.getVertex(TitanId.toVertexId(id1));
                Vertex v2 = g.getVertex(TitanId.toVertexId(id2));
                Edge edge = g.addEdge(null, v1, v2, atype);
                edge.setProperty("timestamp", timestamp);
                edge.setProperty("property", property);
                if (++c%1000L == 0L) {
                    if (c % 100000L == 0) {
                        System.out.println("Processed " + c + " edges from file " + edgeFile
                          + " in " + (System.currentTimeMillis() - startTime) + " ms ("
                          + (c * 1000 / (System.currentTimeMillis() - startTime)) + " edges/s)"
                        );
                    }
                    try {
                        g.commit();
                    } catch (TitanException e) {
                        System.out.print("Commit failed: ");
                        e.printStackTrace();
                    }
                }
            }
        }

        try {
            g.commit();
        } catch (TitanException e) {
            System.out.print("Commit failed: ");
            e.printStackTrace();
        }
        System.out.println("Finished loading nodes from nodeFile " + edgeFile);
        System.out.println("Processed " + c + " edges.");
    }
}
