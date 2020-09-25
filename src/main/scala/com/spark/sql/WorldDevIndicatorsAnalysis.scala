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

    // Data Source . Datasets link given in README.md
    val country_data="C:\\Users\\Sreenivas\\Documents\\Project\\World development\\data\\Country.csv"
    val Indicator_data="C:\\Users\\Sreenivas\\Documents\\Project\\World development\\data\\Indicators.csv"

    // Dataframes creation
    val CountryDf=spark.read.format("csv")
        .option("header","true").option("inferSchema","true").option("path",country_data).load()

    val IndicatorDf=spark.read.format("csv").option("header","true")
        .option("inferSchema","true").option("path",Indicator_data).load()
    // creating Views

    CountryDf.createOrReplaceTempView("Country")
    IndicatorDf.createOrReplaceTempView("Indicators")

    // Problem statements
    //GINI index of China,India,Japan

    val china=spark.sql("select Year,Value as China_GINI_value from Indicators where IndicatorCode='SI.POV.GINI' and CountryCode='CHN' order by Year asc")
    china.show()

    spark.stop()
  }
}