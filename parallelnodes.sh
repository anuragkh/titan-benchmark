NODE_FILES="/path/to/node/files"
export MAVEN_OPTS="-verbose:gc -server -Xmx50g"
mvn exec:java -Dexec.mainClass="edu.berkeley.cs.ParallelLoadNodes" \
  -Dexec.args="${NODE_FILES}"
