package org.octo

import org.apache.spark._
import org.apache.hadoop._

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get}
import org.apache.hadoop.hbase.util.Bytes

import it.nerdammer.spark.hbase._


/** Read from cassandra to make sure things are working.
  * Assumes test.hello table already exists and has data, see cassandra-example.cql
  */


object HBaseHelloWorldExample {
  def main(args: Array[String]): Unit = {
    // only setting app name, all other properties will be specified at runtime for flexibility
    val conf = new SparkConf()
      .setAppName("octo-hbase-helloworld")

    val sc = new SparkContext(conf)

    val hBaseRDD = sc.hbaseTable[(String, Int, String)]("enterprises")
    .select("name", "id")
    .inColumnFamily("info")

    val first = hBaseRDD.first

    sc.stop

    println(first)
  }
}


/*
object HBaseHelloWorldExample {
  def main( args: Array[String] ): Unit = {
    val conf = new HBaseConfiguration()
    val admin = new HBaseAdmin(conf)

    // list the tables
    val listtables=admin.listTables()
    println(listtables)
  }
}
*/
