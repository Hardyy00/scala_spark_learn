package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{MapType, StringType};

object SparkSqlFunctions extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)


  val spark = SparkSession.builder().master("local[*]").appName("Sql Functions").getOrCreate()

  import spark.implicits._;

  val sc = spark.sparkContext;


//  //    date and timestamp
//
//
//  // we need string date in the format of : yyyy-MM-dd
//  val df = Seq("2024-01-31").toDF("Input")
//
//  // current_date() : used to get the current date
//  // date_format() : used to format the date in some other format
//
//  println(sc.appName)
//  df.select(current_date().as("current data"), col("input"), date_format(col("input"), "dd-MM-yyyy").as("formatted date")).show()
//
//
//  // to_date() : converts the date string format x/y/z to date type yyyy-MM-dd
//
//
//  Seq("13/04/2019").toDF("Input").select(col("input"), to_date(col("input"), "dd/MM/yyyy").as("formatted date")).show(false)
//
//
//  // dateDiff() : returns the difference b/w two dates
//
//  Seq(("2019-01-23"), ("2025-06-01"), ("2024-09-19")).toDF("input").select(col("input"),current_date().as("current date"), datediff(current_date(), col("input")).as("diff")).show(false)
//
//
//  // months_between() : returns the months b/w two dates
//
//  Seq("2019-01-23","2024-09-19", "2024-09-04").toDF("date").select(col("date"), datediff(current_date(), col("date")).as("Date diff"), months_between(current_date(), col("date")).as("Months between")).show(false)
//
//  // trunc() : truncates date at a specified unit
//
//  Seq(("2019-01-23"),("2019-06-24"),("2019-09-20")).toDF("date").select(col("date"), trunc(col("date"), "Month").as("Month Trunc"),
//    trunc(col("date"), "year").as("Year Trunc"), trunc(col("date"), "date").as("Date Trunc")).show(false)
//
//
//
//  // add_months(), date_add(),date_sub()
//
//  Seq(("2024-01-23"), ("2024-06-24"),("2024-09-20")).toDF("date").select(col("date"), add_months(col("date"), 4).as("Months add"), date_add(col("date"), 3).as("date add"), date_sub(col("date"),4).as("date sub")).show(false)
//
//  // year(), month(), dayofweek(), dayofmonth(), dayofyear(), next_day(), weekofyear()
//
//
//  Seq(("2024-01-23"), ("2024-06-24"), ("2024-09-20")).toDF("date").select(col("date"), year(col("date")).as("year"), month(col("date")).as("month"), dayofweek(col("date")).as("day of week"), dayofmonth(col("date")).as("day of month") ,
//
//    dayofyear(col("date")).as("day of year"), next_day(col("date"), "Sunday").as("next day"), weekofyear(col("date")).as("week of year")).show(false)
//
//
//  Seq(1).toDF("seq").withColumn("current date", current_date().as("current date")).withColumn("current timestamp", current_timestamp().as("current timestamp")).show(false)

  //////////////////////////////                sorting functions               /////////////////////////////////

  // 1. asc

//  val data = Seq(("John", 25), ("Doe", 22), ("Jane", 29))
//  val df = spark.createDataFrame(data).toDF("Name", "Age")
//
//  df.orderBy(asc("age")).show()


  // 2. desc


  // df.orderBy(desc("age")).show()



  //////////////////////////               aggregate functions                //////////////////////////////////

//  val simpleData = Seq(("James", "Sales", 3000),
//    ("Michael", "Sales", 4600),
//    ("Robert", "Sales", 4100),
//    ("Maria", "Finance", 3000),
//    ("James", "Sales", 3000),
//    ("Scott", "Finance", 3300),
//    ("Jen", "Finance", 3900),
//    ("Jeff", "Marketing", 3000),
//    ("Kumar", "Marketing", 2000),
//    ("Saif", "Sales", 4100)
//  )
//  val df = simpleData.toDF("employee_name", "department", "salary")
//
//  // approx_count_distinct() : returns the count of distinct elements in the group
//
// println(df.select(approx_count_distinct("department")).collect()(0)(0))
//
//
//  // avg : returns the avg of values in the column
//
//  println(df.select(avg("salary")).collect()(0)(0))
//
//  // collect_list() : collect all values from a column  in a list view,, collect_set() : collect all values from a column in a set view
//
//  df.select(collect_set("department").as("deps")).show(false)
//
//  // countDistinct() : returns the number of distinct elements in a columns
//
//  df.select(countDistinct("department", "salary")).show()
//
//  println(df.select(countDistinct("department", "salary")).collect()(0)(0))
//
//
//  // count() : returns number of elements in a column
//
//  println(df.select(count("salary")).collect()(0))
//
//  // first() : returns the first element in a column when ignoreNulls is set to true, it returns the first non-null element
//
//  df.select(first("salary", ignoreNulls = true)).show()
//
//  // max() , min(), mean()
//
//  println(df.select(max("salary")).collect()(0)(0))
//
//
//  // sum(), and sumDistinct()
//
//  println(df.select(sum("salary")).collect()(0)(0))
//
//
//  println(df.select(sumDistinct("salary")).collect()(0)(0))




  ////////////////////////////                              window function                     ////////////////////////////


//  val simpleData = Seq(("James", "Sales", 3000),
//    ("Michael", "Sales", 4600),
//    ("Robert", "Sales", 4100),
//    ("Maria", "Finance", 3000),
//    ("James", "Sales", 3000),
//    ("Scott", "Finance", 3300),
//    ("Jen", "Finance", 3900),
//    ("Jeff", "Marketing", 3000),
//    ("Kumar", "Marketing", 2000),
//    ("Saif", "Sales", 4100)
//  )
//  val df = simpleData.toDF("employee_name", "department", "salary")
//
//
//
//  /// row_number() window function is used to give the sequential row number starting from 1 to the result of each window partition.
//
//  val partition = Window.partitionBy("department").orderBy("salary");
//
//  df.withColumn("rank",row_number.over(partition)).show()
//
//
//  /// rank() window function is used to provide a rank to the result within a window partition. This function leaves gaps in rank when there are ties.
//
//  df.withColumn("rank", rank.over(partition)).show()
//
//  /// dense_rank() window function is used to get the result with rank of rows within a window partition without any gaps. This is similar to rank() function difference being rank function leaves gaps in rank when there are ties.
//
//  df.withColumn("dense rank", dense_rank.over(partition)).show()
//
//  /// ntile() window function returns the relative rank of result rows within a window partition. In below example we have used 2 as an argument to ntile hence it returns ranking between 2 values (1 and 2)
//  // it divides the data for each partition , into a fixed number of buckets and assigns the rank based on the bucket number
//
//  df.withColumn("ntile", ntile(2).over(partition)).show()




  ////////////////////                      json functions                ///////////////////

  val jsonString = """{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}""";

  val data = Seq((1, jsonString))
  """{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
  val df = data.toDF("id", "value")

  df.show(false)


  /// 1. from_json() : converts  json string into struct type or map type

  val dfWithMap = df.withColumn("value", from_json(col("value"), MapType(StringType, StringType)));
  dfWithMap.show()

  // 2. to_json() : converts MapType or struct type to json string

  dfWithMap.withColumn("value", to_json(col("value"))).show(false)



  // 3. json_tuple() : extract the data from json and create them as new columns

 val dfWithJsonTuple = df.select(col("id"), json_tuple(col("value"), "Zipcode","City", "State")).toDF("id", "zipcode", "city", "state")

dfWithJsonTuple.show()

  // 4. get_json_object() : extracts json element from a json string based on json path specified

  df.select(col("id"), get_json_object(col("value") , "$.City").as("city")).show(false)


  // 5. schema_of_json() : create schema string from json string
  spark.range(1).select(schema_of_json(lit("""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}""")).as("schema")).show(false)
















}
