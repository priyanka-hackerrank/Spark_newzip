package com.hackerrank

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, Outcome}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.DataFrame

trait TestSparkSession extends AnyFunSuite with BeforeAndAfterEach {
  val log = Logger.getLogger(this.getClass)
  var _spark: SparkSession = null

  protected override def beforeEach(): Unit = {
    if (_spark == null) {
      val master = "local[2]"
      _spark = SparkSession.builder().master(master).getOrCreate()
    }

    super.beforeEach()
  }

  protected override def afterEach(): Unit = {
    try {
      if (_spark != null) {
        _spark.stop()
        _spark = null
      }
    } finally {
      super.afterEach()
    }
  }

  def createTestDataFrame(data: Array[(String, String)]): DataFrame = {
    import org.apache.spark.sql.types.{StringType, StructField, StructType}
    import org.apache.spark.sql.Row

    val schema = StructType(
      Array(
        StructField("id", StringType, false),
        StructField("order_no", StringType, false)
      )
    )
    val rdd = _spark.sparkContext.parallelize(data)
    val rowRDD = rdd.map(attributes => Row(attributes._1, attributes._2))

    _spark.createDataFrame(rowRDD, schema)
  }
}
