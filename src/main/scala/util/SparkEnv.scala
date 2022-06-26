package util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

class SparkEnv() {
  val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("oneid")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer", "24m")

  lazy val ss: SparkSession =
    SparkSession.builder().config(sparkConf).getOrCreate()

  lazy val sc: SparkContext = {
    val sc = ss.sparkContext
    sc
  }

  lazy val sqlContext: SQLContext = ss.sqlContext

  def executeSQL(sql: String): DataFrame = {
    ss.sql(sql)
  }

  def close(): Unit = {
    ss.close()
  }

  def getSparkSession: SparkSession={
    SparkSession.builder().config(sparkConf).getOrCreate()
  }
}

object SparkEnv {
  def apply(): SparkEnv = new SparkEnv()
}