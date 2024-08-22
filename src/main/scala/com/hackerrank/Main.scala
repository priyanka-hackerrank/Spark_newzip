package com.hackerrank

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Main {
  /**
    * Creates a SparkSession
    *
    * @return SparkSession
    */
  private def initSparkSession(): SparkSession = {
    SparkSession.active
  }

  /**
    * Creates a DataFrame from the CSV file
    *
    * @param csvFile The CSV file
    * @param spark SparkSession to be used in this method
    * @return DataFrame
    */
  def createDataFrame(
      csvFile: String
  )(implicit spark: SparkSession): DataFrame = {
    spark.emptyDataFrame
  }

  /**
    * Fetches unique IDs
    *
    * @param df DataFrame which consists of "id" and "order_no" records
    * @return DataFrame of unique "id" columns
    */
  def distinctID(df: DataFrame): DataFrame = {
    df
  }

  /**
    * Fetches unique Order numbers
    *
    * @param df DataFrame which consists of "id" and "order_no" records
    * @return DataFrame of unique "order_no" columns
    */
  def distinctOrderNo(df: DataFrame): DataFrame = {
    df
  }

  def main(args: Array[String]): Unit = {
    implicit val spark = initSparkSession()

    val inputDf = createDataFrame("data/data.csv")
    inputDf.show()

    val idDf = distinctID(inputDf)
    idDf.show()

    val orderDf = distinctOrderNo(inputDf)
    orderDf.show()
  }
}
