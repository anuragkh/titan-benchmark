package edu.berkeley.cs;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.util.TitanId;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;
import com.tinkerpop.blueprints.util.wrappers.batch.VertexIDType;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class Load {

    public static void main(String[] args) throws ConfigurationException, IOException {
        Configuration config = new PropertiesConfiguration(
                Load.class.getResource("/benchmark.properties"));
        Configuration titanConfiguration = new PropertiesConfiguration(
                Load.class.getResource("/titan-cassandra.properties"));
        load(config, titanConfiguration);

        System.exit(0);
    }

    public static void load(Configuration config, Configuration titanConfig) throws IOException {
        titanConfig.setProperty("storage.batch-loading", true);
        titanConfig.setProperty("schema.default", "none");
        titanConfig.setProperty("graph.set-vertex-id", true);
        titanConfig.setProperty("storage.cassandra.keyspace", config.getString("name"));
        titanConfig.setProperty("storage.batch-loading", true);
        titanConfig.setProperty("storage.buffer-size", 5120);
        titanConfig.setProperty("ids.block-size", 10000000);
        titanConfig.setProperty("ids.authority.wait-time", 60000);

        TitanGraph g = TitanFactory.open(titanConfig);
        createSchemaIfNotExists(g, config);
        if (g.getVertices().iterator().hasNext()) {
            System.err.print("Warning! Graph already has data!");
        } else {
            loadGraph(g, config);
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

    private static void loadGraph(TitanGraph g, Configuration conf) throws IOException {
        BatchGraph<TitanGraph> bg = new BatchGraph<>(g, VertexIDType.NUMBER, 1000);

        int propertySize = conf.getInt("property.size");
        int numProperty = conf.getInt("property.total");
        String nodeFile = conf.getString("data.node");
        String edgeFile = conf.getString("data.edge");
        int offset = conf.getBoolean("zero_indexed") ? 1 : 0;
        System.out.printf("nodeFile %s, edgeFile %s, propertySize %d\n", nodeFile, edgeFile, propertySize);

        long c = 1L;
        try (BufferedReader br = new BufferedReader(new FileReader(nodeFile))) {
            for (String line; (line = br.readLine()) != null; ) {
                // Node file has funky carriage return ^M, so we read one more line to finish the node information
                line += '\02' + br.readLine(); // replace carriage return with dummy line
                Vertex node = bg.addVertex(TitanId.toVertexId(c));
                Iterator<String> tokens = Splitter.fixedLength(propertySize + 1).split(line).iterator();
                for (int i = 0; i < numProperty; i++) {
                    String attr = tokens.next().substring(1); // trim first delimiter character
                    node.setProperty("attr" + i, attr);
                }
                if (++c%1000L == 0L) {
                    System.out.println("Processed " + c + " nodes");
                    bg.commit();
                }
            }
        }

        bg.commit();

        c = 1L;
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

                Vertex v1 = bg.getVertex(TitanId.toVertexId(id1));
                Vertex v2 = bg.getVertex(TitanId.toVertexId(id2));
                Edge edge = bg.addEdge(null, v1, v2, atype);
                edge.setProperty("timestamp", timestamp);
                edge.setProperty("property", property);
                if (++c%1000L == 0L) {
                    System.out.println("Processed " + c + " edges");
                    bg.commit();
                }
            }
        }

        bg.commit();
    }
}
