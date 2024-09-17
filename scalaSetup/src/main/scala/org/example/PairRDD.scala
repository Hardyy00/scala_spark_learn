package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object PairRDD {

  def main(args : Array[String]) : Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession.builder().master("local[*]").appName("Pair RDD").getOrCreate()

    // Tuple RDD
    val rdd = spark.sparkContext.textFile("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/TextFiles/sample.txt").map((f)=> {

      val arr = f.split(",")
      (arr(0), arr(1).toInt)
    })

//    rdd.foreach(println)

    // 1. distinct() : produce distinct key,value pairs

//    rdd.distinct().foreach(println)

    // 2. sortByKey() : return the rdd after sorting by key
//
//    rdd.sortByKey().collect.foreach(println)

    // 3. reduceByKey() : merges the values of each key using a specified associative and commutative reduce function

//    rdd.reduceByKey(_ + _).foreach(println)  // all the  values with same key

    // 4. keys() : returns RDD[K] with all keys in an dataset

//    rdd.keys.collect().foreach(println)

    // values : return RDD[V] with all values in an dataset

//    rdd.values.distinct.sortBy(x=>x).collect().foreach(println)  /// sort distinct values in ascending order


    // 5. flatMapValues() : Applies a function to each value in a key-value pair RDD, producing a new RDD with the same keys but the function results flattened into a single sequence.


//    val rdd2 = spark.sparkContext.parallelize(List(("a", "one two three"), ("b", "four five six")))
//
//    rdd2.flatMapValues(_.split(" ")).foreach(println)


    // 6. groupBykey() : groups the values for each key into an iterable. It does not perform any aggregation
    
    val rdd3 = spark.sparkContext.parallelize(List(("a", 1),("a",2), ("b", 10), ("a",13), ("c",19)))

//    rdd3.groupByKey().collect().foreach(println)

    // 7. mapValues() : applies a function to the values of each keh-value pair without changing the keys

//     rdd3.mapValues(_ * 2).foreach(println)      // double each value in key-value pair

    // 8. sampleKey(withReplacement, fractions) : returns a subset of the rdd, sampling the values for each key with a specified probability

    val rdd4 = spark.sparkContext.textFile("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/TextFiles/User_Data.txt").mapPartitionsWithIndex((idx, itr)=> if(idx == 0) itr.drop(1) else itr)

    val rdd5 = rdd4.map((f)=> {

      val arr = f.split(",")

      (arr(1), arr(2).toInt)
    })
//
//    rdd5.foreach(println)



//    val fraction = Map("Male"-> 0.5, "Female" -> 0.5)
//    val res = rdd5.sampleByKeyExact(withReplacement =  false, fraction)
//
//    val tot = res.count()
//
//    val map = res.countByKey();
//    println(map)
//    println(tot)
//    println(map("Male") + map("Female"))


//    val rdd6 = spark.sparkContext.parallelize(List(("a",1), ("b",2), ("c",3), ("d",4)))
//    val rdd7 = spark.sparkContext.parallelize(List(("b", 10), ("c", 1)))
//
//
//    rdd6.subtractByKey(rdd7).collect().foreach(println)

    val rdd8 = spark.sparkContext.textFile("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/TextFiles/a.txt").map((row)=>{

      val split = row.split(",")

      (split(0), split(1).toInt)
    })

    val rdd9 = spark.sparkContext.textFile("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/TextFiles/b.txt").map((row)=>{

      val split = row.split(",")

      (split(0), split(1).toInt)
    })

    // inner join
//    rdd8.join(rdd9).foreach(println)

    // left outer join
//    rdd8.leftOuterJoin(rdd9).foreach(println)

    // right outer join
//    rdd8.rightOuterJoin(rdd9).collect().foreach(println)

    rdd8.fullOuterJoin(rdd9).foreach(println)






    




  }

}
