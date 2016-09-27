import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.hadoop._

import org.apache.hadoop.hbase.client.{HBaseAdmin, Result, HTable, Get, Put}
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor }
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import org.apache.hadoop.mapreduce.Job

import org.apache.hadoop.hbase.mapred.TableOutputFormat

import org.apache.hadoop.hbase.util.Bytes

import com.github.nscala_time.time.Imports._

import org.apache.hadoop.mapred.{JobConf, OutputFormat}

import org.apache.hadoop.hbase.util.Bytes

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Bytes._
import java.nio.ByteBuffer

object TimeRecommender {


  def convert(t: (String, String, String, Double)) = {
    val rowId = s"P1-${java.util.UUID.randomUUID.toString}"
    val put = new Put(toBytes(rowId))
    put.add(toBytes("info"), toBytes("enterpriseid"), toBytes(t._1))
    put.add(toBytes("info"), toBytes("userid"), toBytes(t._2))
    put.add(toBytes("info"), toBytes("predicted_time"), toBytes(t._3))
    put.add(toBytes("info"), toBytes("score"), toBytes(t._4))
    (new ImmutableBytesWritable, put)
  }

  def myHash(s: String):Array[Int] = {
    val byteArray = s.getBytes
    val padLength = 4 * ((byteArray.length/4) + (if ( byteArray.length%4 > 0 ) 1 else 0))
    val arr = new Array[Byte](padLength)
    for(i <- 0 until padLength-1){
      if (i < byteArray.length) {
        arr(i) = byteArray(i)
      }
      else {
        arr(i) = Byte.box(32)
      }
    }

    println(arr)

    val p = new Array[Int](arr.length/4)

    for(i <- 0 until arr.length/4){
      val _arr = new Array[Byte](4)
      _arr(0) = arr(i)
      _arr(1) = arr(i+1)
      _arr(2) = arr(i+2)
      _arr(3) = arr(i+3)
      val _x = ByteBuffer.wrap(_arr).getInt
      p(i) = _x
      println(_x)
    }
    println(p)

    return p
  }

  def main(args: Array[String]): Unit = {

    // set up environment
    val sparkConf = new SparkConf().setAppName("Octo-Time-Recommender")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    val inputTableName = "user_times"

    conf.set("hbase.master", "localhost:60000")
    conf.setInt("timeout", 120000)
    conf.set(TableInputFormat.INPUT_TABLE, inputTableName)

    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(inputTableName)) {
      println("Required table user_times not found. Aborting!!")
    }
    else {
      val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      println("Number of Records found : " + hBaseRDD.count())

      if (hBaseRDD.count() == 0 ) {
        println("No record found in table user_times. Nothing to do.")
      }
      else {

        val results = hBaseRDD.map(tuple => tuple._2)
        val rows = results.map(result => (
          result.getColumn("info".getBytes(), "enterpriseid".getBytes()),
          result.getColumn("info".getBytes(), "userid".getBytes()),
          result.getColumn("info".getBytes(), "created_at".getBytes())
        ))

        // transform
        val userTimeRDD = rows.map( r => (
          (new String(r._1.get(0).getValue()) + "::" + new String(r._2.get(0).getValue())).hashCode(),
          (new String(r._1.get(0).getValue() )+ "::" + new String(r._3.get(0).getValue())).hashCode()
          )
        )

        // map
        val ut = userTimeRDD.map(t => new Rating(t._1, t._2, 1.0))

        // Build the recommendation model using ALS
        val rank = 10
        val numIterations = 10
        val model = ALS.train(ut, rank, numIterations, 0.01)

        // Evaluate the model on rating data
        val usersTiming = ut.map { case Rating(user, product, rate) =>
          (user, product)
        }

        val predictions = model.predict(userTimeRDD).map { case Rating(user, ts, rate) =>
          ((user, ts), rate)
        }

        val ratesAndPreds = ut.map { case Rating(user, ts, rate) =>
          ((user, ts), rate)
        }.join(predictions)

        val MSE = ratesAndPreds.map { case ((user, ts), (r1, r2)) =>
          val err = (r1 - r2)
          err * err
        }.mean()

        println("Mean Squared Error = " + MSE)


        val preds = predictions.map( p => (
          new String(toBytes(p._1._1), "UTF-32"),
          new String(toBytes(p._1._2), "UTF-32"),
          p._2
        ))

        val jobConfig = new JobConf(conf)
        jobConfig.set(TableOutputFormat.OUTPUT_TABLE, "time_recommendations")

        jobConfig.setOutputFormat(classOf[TableOutputFormat])

        //new PairRDDFunctions(preds.map(convert)).saveAsHadoopDataset(jobConfig)

      }
    }
    sc.stop()}

  }

