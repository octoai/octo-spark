## Prerequisites

- Scala
- **Spark**

This is tested against Spark 2.0.0 for Hadoop 2.7 and Spark 1.6.2 for CDH4. However, we are preferring Spark 2.0.0 and that is what is used for this repo. `SPARK_HOME` here will point to your spark directory

- Sbt
 
### DB Adapters

- [Spark Cassandra Connector](https://github.com/datastax/spark-cassandra-connector). Build the connector for your version of Scala as per the [building docs](https://github.com/datastax/spark-cassandra-connector/blob/master/doc/12_building_and_artifacts.md).


If you get a bunch of compile time issues related to Java ClassNotFoundException, in all likelihood, this is due to Scala version mismatch. You need to make sure that your version of scala is consistent across spark, sbt and connector


## Clone

```bash
git clone git@github.com:octoai/octo-spark.git
```


## Build

```bash
$ sbt clean package
```

## Execute

### Cassandra

```bash
$  ~/etc/spark-2.0.0-bin-hadoop2.6/bin/spark-submit --class com.octo.HelloWorldExample --jars ~/etc/spark-cassandra-connector-assembly-2.0.0-M3.jar --properties-file cassandra.conf target/scala-2.10/octo-spark_2.10-0.0.1.jar 
```

### HBase

We are following the Cloudera package of HBase [http://www.cloudera.com/](http://www.cloudera.com/)

- This is tested against HBase 0.94.15, which can be downloaded from [http://www.cloudera.com/documentation/archive/cdh/4-x/4-7-1/CDH-Version-and-Packaging-Information/cdhvd_topic_3.html](http://www.cloudera.com/documentation/archive/cdh/4-x/4-7-1/CDH-Version-and-Packaging-Information/cdhvd_topic_3.html)

You need to install HBase. Go ahead and install Apache HBase. We will call the HBase home dir as `HBASE_HOME`. The following command provides a relative location to the hbase jar which should be changed as per your directory structure.

```bash
 $SPARK_HOME/bin/spark-submit --class "HBaseHelloWorld" --driver-class-path "$(echo $HBASE_HOME/lib/*.jar |xargs -n1|grep -v 'netty.*\.jar$')" --jars "../../../../etc/hbase-0.94.15-cdh4.7.1/hbase-0.94.15-cdh4.7.1-security.jar" --verbose --properties-file "hbase.conf" target/scala-2.11/octo-spark_2.11-0.0.1.jar
```

