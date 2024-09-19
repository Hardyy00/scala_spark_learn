package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkDataSourceAPI extends  App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().master("local[*]").appName("Data Source API").getOrCreate();
  val sc = spark.sparkContext;

  import spark.implicits._;

//  println(sc.appName)
//
//  val df = spark.read.option("header", true).csv("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/csv/users.csv")
//
//  df.printSchema()
//  df.show()
//
//
//  // option(inferschema, true) : used to infer the datatype of  each field
//  val df2 =spark.read.option("header", true).option("inferschema", true).csv("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/csv/zipcodes.csv")
//
//  df2.printSchema()
//  df2.show()
//
//
//  // read multiple csv
////  val df3 = spark.read.csv("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/csv")
////
////  df3.show(500)
//
//
//  df2.write.options(Map("header"-> "true", "delimiter"-> "," )).csv("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/csv/test.csv")


  // reading a json file scattered over multiple lines
//  val df = spark.read.option("multiline", true).json("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/json/users.json");
//
//  df.printSchema()
//  df.show(false)
//
//  val df2 = spark.read.options(Map("inferschema"-> "true")).json("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/json/zipcodes.json");
//  df2.printSchema()
//  df2.show()


  //////////////////////////////                read and write parquet file                 ////////////////////////////

  val data = Seq(("James ","","Smith","36636","M",3000),
    ("Michael ","Rose","","40288","M",4000),
    ("Robert ","","Williams","42114","M",4000),
    ("Maria ","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1))

  val columns = Seq("firstname","middlename","lastname","dob","gender","salary")


  val df = data.toDF(columns : _*)
//
//  df.write.parquet("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/park.parquet")

  spark.read.parquet("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/park.parquet").show()


//  df.write.partitionBy("gender", "salary").parquet("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/park2.parquet")


  df.createOrReplaceTempView("Table")

  spark.sql("select * from Table where gender='M' and salary > 3000").show(false)




}
