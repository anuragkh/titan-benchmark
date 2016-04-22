EDGE_FILES="/path/to/edge/files/*"
export MAVEN_OPTS="-verbose:gc -server -Xmx50g"
mvn exec:java -Dexec.mainClass="edu.berkeley.cs.ParallelLoadEdges" \
  -Dexec.args="${EDGE_FILES}"
