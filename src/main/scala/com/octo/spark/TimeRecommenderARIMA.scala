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
import com.github.nscala_time.time._
import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}

import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types._

import org.apache.hadoop.mapred.{JobConf, OutputFormat}

import org.apache.hadoop.hbase.util.Bytes

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Bytes._
import java.nio.ByteBuffer
import com.cloudera.sparkts.models.ARIMA

import com.cloudera.sparkts._
import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests

object TimeRecommenderARIMA {

  def convertStringToTimestamp(ts: String, fmt: String): Timestamp = {
    Timestamp.from(new RichString(ts).dateTimeFormat(fmt).toDate().toInstant())
  }

  def genEnterpriseUserKey(eid: String, uid: String): String = {
    eid + "::" + uid
  }

  def getDetailsFromKey(key: String): Array[String] = {
    key.split("::")
  }

  def loadObservations(sc: SparkContext, conf: org.apache.hadoop.conf.Configuration, spark: SparkSession): DataFrame = {
    val inputTableName = "user_times"

    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(inputTableName)) {
      throw new IllegalArgumentException("Required table user_times not found. Aborting!!");
    }
    else {
      val hBaseRDD = sc.newAPIHadoopRDD(conf,
        classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result]
      )
      println("Number of Records found : " + hBaseRDD.count())

      if (hBaseRDD.count() == 0 ) {
        throw new IllegalArgumentException("No records found.")
      }
      else {

        // get raw data from hbase
        val results = hBaseRDD.map(tuple => tuple._2)
        val rows = results.map(result => (
          result.getColumn("info".getBytes(), "enterpriseid".getBytes()),
          result.getColumn("info".getBytes(), "userid".getBytes()),
          result.getColumn("info".getBytes(), "created_at".getBytes())
        ))

        // transform into a single key, ts
        val userTimeRDD = rows.map( r => org.apache.spark.sql.Row(
          convertStringToTimestamp(new String(r._3.get(0).getValue()),  "EEE MMM DD HH:mm:ss Z YYYY"), //thats how hbase stores data
          genEnterpriseUserKey(new String(r._1.get(0).getValue()), new String(r._2.get(0).getValue())),
          1.0
          )
        )
        val fields = Seq(
          StructField("timestamp", TimestampType, true),
          StructField("key", StringType, true),
          StructField("activity", DoubleType, true)
        )
        val schema = StructType(fields)

        spark.createDataFrame(userTimeRDD, schema)
        //val sqlContext = new SQLContext(sc)
        //sqlContext.createDataFrame(userTimeRDD, schema)
      }
    }
  }


  def convert(key: String, preds: Vector[Double]) = {
    val rowId = s"P1-${java.util.UUID.randomUUID.toString}"
    val _key = key.split("::")
    val eid = _key._1
    val uid = _key._2

    val _ts = DateTime.now

    val _p = preds.toArray

    val times = new Array[String](_p.length)

    for( i <- 0 until _p.length) {
      if(_p(i)) {
        times(i) = (_ts + (i * 15)).toString // converted
      }
    }
    val put = new Put(toBytes(rowId))
    put.add(toBytes("info"), toBytes("enterpriseid"), toBytes(eid))
    put.add(toBytes("info"), toBytes("userid"), toBytes(uid))
    put.add(toBytes("info"), toBytes("next_times"), toBytes(times.mkString))
    put.add(toBytes("info"), toBytes("score"), toBytes("1"))
    (new ImmutableBytesWritable, put)
  }


  def main(args: Array[String]): Unit = {

    val appName = "Octo-TimeRecommender-ARIMA"

    // set up environment
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)

    val inputTableName = "user_times"

    val spark = SparkSession
      .builder()
      .appName(appName)
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val conf = HBaseConfiguration.create()
    conf.set("hbase.master", "localhost:60000")
    conf.setInt("timeout", 120000)
    conf.set(TableInputFormat.INPUT_TABLE, inputTableName)

    val userTimeObs = loadObservations(sc, conf, spark)

    // Create a 5 minute index over past 1 week
    val zone = ZoneId.systemDefault()
    val dtIndex = DateTimeIndex.uniformFromInterval(
      ZonedDateTime.ofInstant((DateTime.now - 2.weeks).toDate().toInstant(), zone),
      ZonedDateTime.ofInstant(DateTime.now.toDate().toInstant(), zone),
      new MinuteFrequency(15)
    )

    val smRdd = TimeSeriesRDD.timeSeriesRDDFromObservations(
      dtIndex,
      userTimeObs,
      "timestamp",
      "key",
      "activity"
    )

    val rowd = smRdd.toInstants()

    //rowd.map()

    smRdd.cache()

    // map each to the arima model
    val preds = smRdd.map( r => {
        val model = ARIMA.fitModel(100, 100, 1, r._2)
        // forecase for next 200 times and find the uniques
        (r._1, model.forecast(r._2, 200))
      })

    val jobConfig = new JobConf(conf)
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, "time_recommendations")

    jobConfig.setOutputFormat(classOf[TableOutputFormat])

    new PairRDDFunctions(preds.map(convert)).saveAsHadoopDataset(jobConfig)


    sc.stop()
  }
}
