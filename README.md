## Prerequisites

- Scala
- Spark
- Sbt
 
### DB Adapters

- [Spark Cassandra Connector](https://github.com/datastax/spark-cassandra-connector). Build the connector for your version of Scala as per the [building docs](https://github.com/datastax/spark-cassandra-connector/blob/master/doc/12_building_and_artifacts.md).

If you get a bunch of compile time issues related to Java ClassNotFoundException, in all likelihood, this is due to Scala version mismatch. You need to make sure that your version of scala is consistent across spark, sbt and connector

### Build

```bash
$ sbt clean package
```

### Execute

```bash
$ $SPARK_HOME/bin/spark-submit --class org.octo.HelloWorldExample --jars ~/etc/spark-cassandra-connector-assembly-2.0.0-M3.jar --properties-file cassandra.conf target/scala-2.11/octo-spark_2.11-1.0.jar 
```

