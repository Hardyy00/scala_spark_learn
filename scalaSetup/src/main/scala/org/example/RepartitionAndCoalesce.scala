package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object RepartitionAndCoalesce {
  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local[*]").appName("Repartition and Coalesce").getOrCreate();

    val rdd = spark.sparkContext.parallelize(Range(0, 20), 9) // there will be 9 partitions

    //    println(rdd.partitions.length)

    val rddFromFile = spark.sparkContext.textFile("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/TextFiles/a.txt", 10)
    //    println(rddFromFile.partitions.length)

    //    rddFromFile.saveAsTextFile("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/savedFiles")


    //    val repartitionedRdd = rdd.coalesce(10)  // reduce the number of partitions
    //
    //    println(repartitionedRdd.partitions.length)



//    println(rdd.coalesce(5).partitions.length)
//
//
//    println(rdd.repartition(22).partitions.length) // increase or decrease the number of partitions  (very costly)


    // repartition() and coalesce() in dataframes
    //    val df = spark.range(0,20)
    //
    //    df.write.mode(SaveMode.Overwrite).csv("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/TextFiles/c.csv")
    //
    //    println(df.rdd.partitions.length)
    //
    //    println(df.coalesce(2).rdd.partitions.length)

    import spark.implicits._

//    val simpleData = Seq(("James", "Sales", "NY", 90000, 34, 10000),
//      ("Michael", "Sales", "NY", 86000, 56, 20000),
//      ("Robert", "Sales", "CA", 81000, 30, 23000),
//      ("Maria", "Finance", "CA", 90000, 24, 23000),
//      ("Raman", "Finance", "CA", 99000, 40, 24000),
//      ("Scott", "Finance", "NY", 83000, 36, 19000))
//
//    val df = simpleData.toDF("employee_name", "department", "state", "salary", "age", "bonus")
//
//    val df2 = df.select("department", "state").groupBy("department", "state") .count()
//    df2.show()
//
//    println(df2.rdd.getNumPartitions)
//
//
//    val persistentData = df.persist(StorageLevel.MEMORY_ONLY_SER)

//     broadcast variable

//    val broadCastVar =spark.sparkContext.broadcast(Array(1,2,3,4,5,6,7,8,9))
//
//    println(broadCastVar.value.mkString(","))

//    val countryMap = Map( "USA"-> "United States of America")
//    val statesMap = Map("CA" -> "California", "NY"-> "New York", "FL"-> "Florida")
//
//    val broadCastCountry = spark.sparkContext.broadcast(countryMap)
//    val broadCastState = spark.sparkContext.broadcast(statesMap)
//
//
//    val data = Seq(("James","Smith","USA","CA"),
//      ("Michael","Rose","USA","NY"),
//      ("Robert","Williams","USA","CA"),
//      ("Maria","Jones","USA","FL")
//    )
//
//    val columns = Seq("firstName", "lastName", "country", "state")
//
//    val df = data.toDF(columns: _*)
//
////    df.show()
//
//    val df2 = df.map((row)=>{
//
//      val country = row.getString(2)
//      val state = row.getString(3)
//
//      val fullCountry = broadCastCountry.value.get(country).get
//      val fullState = broadCastState.value.get(state).get
//
//      (row.getString(0), row.getString(1), broadCastCountry.value(country), broadCastState.value(state))
//    }).toDF(columns:_*)
//
//    df2.show(false)  // don't truncate the answer

    // accumulators : longAccumulator , doubleAccumulator , collectorAccumulator

//    val acc = spark.sparkContext.longAccumulator("Acc")
//
//    spark.sparkContext.parallelize(Array(0,1,2,3,4,5)).foreach((x)=> acc.add(x))
//    println(acc.value)


    // creating dataframe from rdd

//    val data = Seq(("James","Smith","USA","CA"),
//          ("Michael","Rose","USA","NY"),
//          ("Robert","Williams","USA","CA"),
//          ("Maria","Jones","USA","FL")
//        )
//
//
//
//    val df = spark.sparkContext.parallelize(data).toDF(Seq("firstName", "lastName", "country", "state") : _*)  // using toDF method
//
//    df.show()
//
//    df.printSchema()


    // converting

//    val schema = StructType(StructField("alp", StringType, nullable = false) :: StructField("num", IntegerType, nullable = false) :: Nil)
//
//    val rdd4 = spark.sparkContext.textFile("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/TextFiles/a.txt").map((s)=> {
//      val arr = s.split(",")
//      Row(arr(0), arr(1).toInt)
//    })
//    val df = spark.createDataFrame(rdd4, schema)
//
//    df.show()
//
//
//
//    spark.sparkContext.textFile("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/TextFiles/a.txt").repartition(100).saveAsTextFile("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/ok")


    val ds = spark.sparkContext.textFile("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/TextFiles/a.txt").map((row)=>{

      val arr = row.split(",")
      (arr(0), arr(1).toInt)
    }).toDS()

    ds.show()


  }
}