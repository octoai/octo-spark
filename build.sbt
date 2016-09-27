name := "octo-spark"

version := "0.0.1"

organization := "octo"

spName := "octo/octospark"

scalaVersion := "2.11.8"

sparkVersion := "2.0.0"

sparkComponents += "mllib"

sparkComponents ++= Seq("streaming", "sql")

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
    "org.apache.hadoop" % "hadoop-core" % "1.2.1",
    "org.apache.hbase" % "hbase" % "0.94.15",
    "com.github.nscala-time" % "nscala-time_2.10" % "2.14.0",
    "com.cloudera.sparkts" % "sparkts" % "0.4.0"
)

dependencyOverrides += "org.codehaus.jackson" % "jackson-mapper-asl" % "1.8.4"

spAppendScalaVersion := true
