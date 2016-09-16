name := "octo-spark"

version := "1.0"

organization := "octo"

scalaVersion := "2.10.5"

sparkVersion := "2.0.0"

sparkComponents += "mllib"


/*
  This dependency is for cassandra. If you are not building anything for
  cassandra, it may be removed safely. Any errors you will get in compile
  time, must be resolved by removing the classes that refernce this
  dependency
*/
spDependencies += "datastax/spark-cassandra-connector:2.0.0-M2-s_2.10"

resolvers += "Apache HBase" at "https://repository.apache.org/content/repositories/releases"

resolvers += "Thrift" at "http://people.apache.org/~rawson/repo/"

libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-core" % "0.20.2",
    "org.apache.hbase" % "hbase" % "0.90.4"
)

libraryDependencies += "it.nerdammer.bigdata" % "spark-hbase-connector_2.10" % "1.0.3"
