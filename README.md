## Prerequisites

- Scala
- Spark
- Sbt
 
### DB Adapters

- [Spark Cassandra Connector](https://github.com/datastax/spark-cassandra-connector). Build the connector for your version of Scala as per the [building docs](https://github.com/datastax/spark-cassandra-connector/blob/master/doc/12_building_and_artifacts.md).
- Spark HBase Connector. Build the connector for your version of Scala. 
  - [2.10](https://github.com/nerdammer/spark-hbase-connector)
  - [2.11](https://github.com/octoai/spark-hbase-connector)


If you get a bunch of compile time issues related to Java ClassNotFoundException, in all likelihood, this is due to Scala version mismatch. You need to make sure that your version of scala is consistent across spark, sbt and connector

### Build

```bash
$ sbt clean package
```

## Execute

### Cassandra

```bash
$ $SPARK_HOME/bin/spark-submit --class org.octo.HelloWorldExample --jars $JARS_HOME/spark-cassandra-connector-assembly-2.0.0-M3.jar --properties-file cassandra.conf target/scala-2.11/octo-spark_2.11-1.0.jar 
```

### HBase

You need to install HBase. Go ahead and install Apache HBase. We will call the HBase home dir as `HBASE_HOME`

```bash
$SPARK_HOME/bin/spark-submit --class org.octo.HBaseHelloWorldExample --driver-class-path $(echo $HBASE_HOME/lib/h*.jar | tr ' ' ':') --properties-file hbase.conf --jars ~/etc/spark-hbase-connector_2.10-1.0.3.jar  --verbose target/scala-2.10/octo-spark_2.10-1.0.jar
```

