import org.apache.spark._
import org.apache.hadoop._

import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor }
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get}
import org.apache.hadoop.hbase.util.Bytes

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

object ProductRecommender {

  def main(args: Array[String]): Unit = {

    // set up environment
    val sparkConf = new SparkConf().setAppName("Octo-Product-Recommender")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    val tableName = "product_page_views"

    conf.set("hbase.master", "localhost:60000")
    conf.setInt("timeout", 120000)
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    // some error with following code. When used all scans return 0
    //conf.set(TableInputFormat.SCAN_COLUMNS, "info:productid,info:userid")

    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      println("Required table product_page_views not found. Aborting!!")
    }
    else {
      val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      println("Number of Records found : " + hBaseRDD.count())

      if (hBaseRDD.count() == 0 ) {
        println("No record found in table product_page_views. Nothing to do.")
      }
      else {

        val results = hBaseRDD.map(tuple => tuple._2)
        val rows = results.map(result => (
          result.getColumn("info".getBytes(), "userid".getBytes()),
          result.getColumn("info".getBytes(), "productid".getBytes())
        ))

        // rows is java.util.List[org.apache.hadoop.hbase.KeyValue]
        // and it's list is a single element list, which contain the
        // key, value of information. So, we fetch the first element
        // and then fetch that element's value
        //
        // data collection format org.apache.spark.rdd.RDD[(String, String)]
        // which is userid:productid in the order that we are fetching
        val userProductRDD = rows.map( r => (
          new String(r._1.get(0).getValue()).hashCode(),
          new String(r._2.get(0).getValue()).hashCode()
          )
        )

        // create Rating RDD. We rate a product as 1.0 when the user viewed it
        val userProductRatings = userProductRDD.map( r => new Rating(r._1, r._2, 1.0))

        // Build the recommendation model using ALS
        val rank = 10
        val numIterations = 10
        val model = ALS.train(userProductRatings, rank, numIterations, 0.01)

        // Evaluate the model on rating data
        val usersProducts = userProductRatings.map { case Rating(user, product, rate) =>
          (user, product)
        }

        val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>
          ((user, product), rate)
        }

        val ratesAndPreds = userProductRatings.map { case Rating(user, product, rate) =>
          ((user, product), rate)
        }.join(predictions)

        val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
          val err = (r1 - r2)
          err * err
        }.mean()

        println("Mean Squared Error = " + MSE)

        // save to hbase
        model.save(sc, "/tmp/userproductCFModel")
      }
    }
    sc.stop()
  }
}

