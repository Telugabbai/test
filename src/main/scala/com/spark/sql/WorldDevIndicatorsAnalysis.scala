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
    val CountryDf=spark.read.format("csv").option("header","true").option("inferSchema","true").option("path",country_data).load()

    val IndicatorDf=spark.read.format("csv").option("header","true").option("inferSchema","true").option("path",Indicator_data).load()
    // creating Views

    CountryDf.createOrReplaceTempView("Country")
    IndicatorDf.createOrReplaceTempView("Indicators")

    // Problem statements
   // Problem statement 1
    //GINI index of China(CHN),India(IND),Japan(JPN)

    val china_gini=spark.sql("select Year,Value as China_GINI_value from Indicators where IndicatorCode='SI.POV.GINI' and CountryCode='CHN' order by Year asc")
    val india_gini=spark.sql("select Year,Value as India_GINI_value from Indicators where IndicatorCode='SI.POV.GINI' and CountryCode='IND' order by Year asc")
    val japan_gini=spark.sql("select Year,Value as Japan_GINI_value from Indicators where IndicatorCode='SI.POV.GINI' and CountryCode='JPN' order by Year asc")

    china_gini.show()
    india_gini.show()
    japan_gini.show()

//    Problem statement 2
//    Youth Literacy Rate
    val query="select CountryName,Value as Youth_Literacy_Rate from Indicators I JOIN Country C ON I.CountryCode=C.CountryCode" +
  " where IndicatorCode='SE.ADT.1524.LT.ZS' and Year=1990 order by Youth_Literacy_Rate desc"
    val pb2=spark.sql(query)
      pb2.show()

//    Problem statement 3
//    Trade as Percentage of GDP for China,India and Japan
    val pb3_query="select Year,CountryName,Value from Indicators where IndicatorCode='NE.TRD.GNFS.ZS' and CountryCode " +
  " in ('CHN','IND','JPN')  order by Year"

    val pb3=spark.sql(pb3_query)
    pb3.show()

//    Problem statement 4
//    Exports of goods and services China,India and Japan based on constant 2005 US$
    val pb4=spark.sql(" select Year,CountryName,value from Indicators where IndicatorCode='NE.EXP.GNFS.KD' and CountryCode in ('CHN','IND','JPN') order by Year")
    pb4.show()

    // To know Types of Exports of goods and services
//    val samp=spark.sql("select IndicatorName,IndicatorCode from Indicators where IndicatorCode like 'NE.EXP.GNFS.%' group by IndicatorCode,IndicatorName")
//      samp.show()


//    Problem statement 5
//    Imports of goods and services China,India and Japan
    val pb5=spark.sql(" select Year,CountryName,value from Indicators where IndicatorCode='NE.IMP.GNFS.KD' and CountryCode in ('CHN','IND','JPN') order by Year")
        pb5.show()


//    Problem statement 6
//    GDP for capita
  val pb6=spark.sql(" select Year,CountryName,value from Indicators where IndicatorCode='NY.GDP.MKTP.KN' and CountryCode in ('CHN','IND','JPN') order by Year")
    pb6.show()

//    Problem statement 7
//    Poverty Alleviation
    val pb7=spark.sql(" select Year,CountryName,value from Indicators where IndicatorCode='SI.POV.2DAY' and CountryCode in ('CHN','IND','JPN') order by Year")
       pb7.show()


//    Problem statement 8
//    Life Expectancy at birth
      val pb8=spark.sql(" select Year,CountryName,IndicatorName,value from Indicators where IndicatorCode in ('SP.DYN.LE00.FE.IN','SP.DYN.LE00.MA.IN') and CountryCode in ('CHN','IND','JPN') order by Year")
        pb8.show()

    //    Problem statement 9
    //    Urban population growth
    val pb9=spark.sql(" select Year,CountryName,value from Indicators where IndicatorCode='SP.URB.TOTL.IN.ZS' and CountryCode in ('CHN','IND','JPN') order by Year")
        pb9.show()


//    Problem statement 10
//    Infant Morality
    val pb10=spark.sql(" select Year,CountryName,value from Indicators where IndicatorCode='SP.DYN.IMRT.IN' and CountryCode in ('CHN','IND','JPN') order by Year")
        pb10.show()


//    Problem statement 11
//      The 10 counties with lowest Average Income in 1962/2014
    val low62=spark.sql("select CountryName,value from Indicators where IndicatorCode='NY.GNP.PCAP.CD' and Year=1962 order by Value asc limit 10")
      low62.show()
    // 2014
    val low14=spark.sql("select CountryName,value from Indicators where IndicatorCode='NY.GNP.PCAP.CD' and Year=2014 order by Value asc limit 10")
    low14.show()


    //    Problem statement 12
//    The 10 counties with highest Average Income in 1962/2014
    val high62=spark.sql("select CountryName,value from Indicators where IndicatorCode='NY.GNP.PCAP.CD' and Year=1962 order by Value desc limit 10")
    high62.show()

    val high14=spark.sql("select CountryName,value from Indicators where IndicatorCode='NY.GNP.PCAP.CD' and Year=2014 order by Value desc limit 10")
    high14.show()


    //    Problem statement 13
//    Average income from 1960- 2014 in few Rich countries
    val pb13_query="select Year,CountryName,Value from Indicators where IndicatorCode='NY.GNP.PCAP.CD' and CountryName in ('Australia','Austria','Canada','Luxembourg','Netherlands','Norway','United States')"
    val pb13=spark.sql(pb13_query)
    pb13.show()


//    Problem statement 14
//    Average income from 1960- 2014 in few Poor countries
        val pb14_query="select Year,CountryName,Value from Indicators where IndicatorCode='NY.GNP.PCAP.CD' and CountryName in ('Burundi','Togo','Malawi','Central African Republic')"
       val pb14=spark.sql(pb14_query)
       pb14.show()


//    Problem statement 15
//    Average income in 1962 and 2014 of rich countries
    //1962
    val pb15_1=spark.sql("select CountryName,Year,Value from Indicators where IndicatorCode in ('NY.GNP.PCAP.CD') and Year=1962 and CountryName in ('Norway','Togo','Malawi','China','India')")
      pb15_1.show()
    // 2014
    val pb15_2=spark.sql("select CountryName,Year,Value from Indicators where IndicatorCode in ('NY.GNP.PCAP.CD') and Year=2014 and CountryName in ('Norway','Togo','Malawi','China','India')")
      pb15_2.show()

//    Problem statement 16
//      Life expectancy in France 1960 to 2013 SP.DYN.LE00.FE.IN
    val pb16=spark.sql("select Year,Value from Indicators where IndicatorCode='SP.DYN.LE00.FE.IN' and CountryName='France'")
      pb16.show()

//    Problem statement 17
//    G-5 Countries birth rates in 1960-2013
    val pb17=spark.sql("select Year,Value from Indicators where IndicatorCode='SP.DYN.CBRT.IN' and CountryName in ( 'Brazil','China','India','Mexico','South Africa')")
    pb17.show()

//    Problem statement 18
//    G-7 Countries birth rates in 1960-2013
      val pb18=spark.sql("select Year,Value from Indicators where IndicatorCode='SP.DYN.CBRT.IN' and CountryName in ('Canada','France','Germany','United Kingdom','Italy','Japan','United States')")
          pb18.show()

//    Problem statement 19
//    World per capita income in 2013 (constant 2005 US$)
     val pb19=spark.sql("select Year,CountryName,int(Value) from Indicators where IndicatorCode ='NY.ADJ.NNTY.PC.KD' and Year=2013 order by Value desc limit 200")
    pb19.show()
    spark.stop()
  }
}