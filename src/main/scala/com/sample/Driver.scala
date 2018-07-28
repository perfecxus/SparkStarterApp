package com.sample

import org.apache.spark.sql.SparkSession

/**
  * Created by sinchan on 28/07/18.
  */
object Driver {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MySatrtSparkApp").master("local[2]").getOrCreate()

    val fileRdd = spark.sparkContext.textFile("/Users/sinchan/Documents/Coursera/BigData & MachineLearning/big-data-4/daily_weather.csv")

    println(fileRdd.getClass.getName)
    println("No of partitions: "+fileRdd.partitions.length)

    val rdd2 = fileRdd.map(x=>x.split(","))
    println(rdd2.getClass.getName)

    val header = rdd2.first()
    println(header.mkString(" | "))
    val rdd3 = rdd2.filter(row => row.mkString("|") != header.mkString("|"))

    //println(rdd3.collect().mkString(","))

    println("======================================================================")

   val rdd3a = rdd3.filter(row => !row(1).isEmpty & !row(2).isEmpty  )
   val rdd4 = rdd3a.map(row => (row(1).toDouble.toInt,row(2).toDouble))


    //rdd4.foreach(println)

    val rdd5 = rdd4.reduceByKey((val1,val2) => (val1 + val2)/2)

    rdd5.foreach(println)
  //rdd3.map(row => row(0))


    //fileRdd.foreach(println)
    println("======================================================================")
    //rdd2.foreach(println)

    //println(rdd2.toDebugString)
  }

}
