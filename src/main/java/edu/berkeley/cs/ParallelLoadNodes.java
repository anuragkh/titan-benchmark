package edu.berkeley.cs;

import com.google.common.base.Splitter;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.util.TitanId;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ParallelLoadNodes {

  private static int propertySize;
  private static int numProperty;
  private static String[] nodeFiles;

  public static void main(String[] args) throws ConfigurationException, IOException {
    Configuration config =
      new PropertiesConfiguration(ParallelLoadNodes.class.getResource("/benchmark.properties"));

    propertySize = config.getInt("property.size");
    numProperty = config.getInt("property.total");
    nodeFiles = args;

    Configuration titanConfiguration = new PropertiesConfiguration(
      ParallelLoadNodes.class.getResource("/titan-cassandra.properties"));

    load(config, titanConfiguration);

    System.exit(0);
  }

  public static int countLines(String filename) throws IOException {
    try (InputStream is = new BufferedInputStream(new FileInputStream(filename))) {
      byte[] c = new byte[1024];
      int count = 0;
      int readChars;
      boolean endsWithoutNewLine = false;
      while ((readChars = is.read(c)) != -1) {
        for (int i = 0; i < readChars; ++i) {
          if (c[i] == '\n')
            ++count;
        }
        endsWithoutNewLine = (c[readChars - 1] != '\n');
      }
      if (endsWithoutNewLine) {
        ++count;
      }
      return count;
    }
  }

  public static void load(Configuration config, Configuration titanConfig) throws IOException {
    titanConfig.setProperty("storage.batch-loading", true);
    titanConfig.setProperty("schema.default", "none");
    titanConfig.setProperty("graph.set-vertex-id", true);
    titanConfig.setProperty("storage.cassandra.keyspace", config.getString("name"));
    titanConfig.setProperty("storage.buffer-size", 5120);
    titanConfig.setProperty("ids.block-size", 1000000);

    TitanGraph g = TitanFactory.open(titanConfig);
    createSchemaIfNotExists(g, config);

    long lineCount = 0;
    List<Thread> threads = new ArrayList<>(nodeFiles.length);
    for (String nodeFile : nodeFiles) {
      long seed = 1L + lineCount;
      threads.add(new Thread(new NodeLoader(g, nodeFile, seed)));
      lineCount += countLines(nodeFile);
    }

    for (Thread thread : threads) {
      thread.start();
    }

    try {
      for (Thread thread : threads) {
        thread.join();
      }
    } catch (InterruptedException e) {
      System.err.println("NodeLoader thread failed: ");
      e.printStackTrace();
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
      EdgeLabel label =
        mgmt.makeEdgeLabel("" + i).signature(timestamp, edgeProperty).unidirected().make();
      if (config.getBoolean("index_timestamp")) {
        mgmt.buildEdgeIndex(label, "byEdge" + i, Direction.OUT, Order.DESC, timestamp);
      }
    }

    mgmt.commit();
  }

  private static void loadNodes(TitanGraph g, String nodeFile, long seed) throws IOException {
    System.out.println("Loading nodeFile " + nodeFile);

    long c = seed;
    long startTime = System.currentTimeMillis();
    try (BufferedReader br = new BufferedReader(new FileReader(nodeFile))) {
      for (String line; (line = br.readLine()) != null; ) {
        // Node file has funky carriage return ^M, so we read one more line to finish the node information
        line += '\02' + br.readLine(); // replace carriage return with dummy line
        Vertex node = g.addVertex(TitanId.toVertexId(c));
        Iterator<String> tokens = Splitter.fixedLength(propertySize + 1).split(line).iterator();
        for (int i = 0; i < numProperty; i++) {
          String attr = tokens.next().substring(1); // trim first delimiter character
          node.setProperty("attr" + i, attr);
        }
        if (++c % 1000L == 0L) {
          if (c % 100000L == 0L) {
            System.out.println(
              "Processed " + (c - seed) + " nodes from file " + nodeFile + " in " + (
                System.currentTimeMillis() - startTime) + "ms (" + ((c - seed) * 1000.0 / (
                System.currentTimeMillis() - startTime)) + " nodes/s)");
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
    System.out.println("Processed " + (c - seed) + " nodes.");
  }


  static class NodeLoader implements Runnable {

    TitanGraph g;
    String nodeFile;
    long seed;

    public NodeLoader(TitanGraph g, String nodeFile, long seed) {
      this.g = g;
      this.nodeFile = nodeFile;
      this.seed = seed;
    }

    @Override public void run() {
      try {
        loadNodes(g, nodeFile, seed);
      } catch (IOException e) {
        System.err.println("NodeLoader thread failed: ");
        e.printStackTrace();
      }

    }
  }
}
