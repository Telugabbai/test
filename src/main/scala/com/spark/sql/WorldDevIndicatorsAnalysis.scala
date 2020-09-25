package com.spark.sql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object WorldDevIndicatorsAnalysis {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("WorldDevIndicatorsAnalysis").getOrCreate()
    // val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    spark.stop()
  }
}