package com.octo.spark

import org.apache.spark._
import com.datastax.spark.connector._

/** Read from cassandra to make sure things are working.
  * Assumes test.hello table already exists and has data, see cassandra-example.cql
  */
object HelloWorldExample {
  def xmain(args: Array[String]): Unit = {
    // only setting app name, all other properties will be specified at runtime for flexibility
    val conf = new SparkConf()
      .setAppName("octo-helloworld")

    val sc = new SparkContext(conf)

    val hello = sc.cassandraTable[(String, String)]("octo_development", "enterprises")

    val first = hello.first

    sc.stop

    println(first)
  }
}
