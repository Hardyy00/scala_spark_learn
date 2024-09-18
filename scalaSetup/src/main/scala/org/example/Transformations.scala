package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Transformations extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().master("local[*]").appName("Transformations").getOrCreate()

  val data = List(List(1,2,3),List(4,5,6))

  val rdd = spark.sparkContext.parallelize(data).flatMap((x)=> x)

  println(rdd.collect().mkString(","))

  spark.sparkContext.textFile("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/TextFiles/b.txt").map((s)=>{

    val ar = s.split(",")
    (ar(0),ar(1).toInt)
  }).sortBy(_._2).collect().foreach(println)




  








}
