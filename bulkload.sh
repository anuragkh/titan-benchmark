export MAVEN_OPTS="-verbose:gc -server -Xmx50g"
mvn exec:java -Dexec.mainClass="edu.berkeley.cs.Load"
