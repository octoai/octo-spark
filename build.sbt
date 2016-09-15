name := "octo-spark"

version := "1.0"

organization := "octo"

scalaVersion := "2.11.8"

sparkVersion := "2.0.0"

sparkComponents += "mllib"

spDependencies += "datastax/spark-cassandra-connector:2.0.0-M2-s_2.10"
