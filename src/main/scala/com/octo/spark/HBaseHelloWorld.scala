import org.apache.spark._
import org.apache.hadoop._

import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor }
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get}
import org.apache.hadoop.hbase.util.Bytes

/*

import it.nerdammer.spark.hbase._

object HBaseHelloWorld {
  def main(args: Array[String]): Unit = {
    // only setting app name, all other properties will be specified at runtime for flexibility
    val conf = new SparkConf()
      .setAppName("octo-hbase-helloworld")

    val sc = new SparkContext(conf)

    val hBaseRDD = sc.hbaseTable[(String, String, String)]("enterprises")
    .select("name", "id")
    .inColumnFamily("info")

    val first = hBaseRDD.first

    sc.stop

    println(first)
  }
}
*/


object HBaseHelloWorld {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseRead")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    val tableName = "enterprises"

    conf.set("hbase.master", "localhost:60000")
    conf.setInt("timeout", 120000)
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(tableName)
      admin.createTable(tableDesc)
    }

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    println("Number of Records found : " + hBaseRDD.count())

    sc.stop()
  }
}

