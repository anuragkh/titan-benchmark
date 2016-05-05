export MAVEN_OPTS="-verbose:gc -server -Xmx50g"
mvn clean compile
sed -i -e 's/-Xmx256M/-Xmx8G/g' /usr/bin/sstableloader
mvn exec:java -Dexec.mainClass="edu.berkeley.cs.Load"
