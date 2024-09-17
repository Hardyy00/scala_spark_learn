import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main{
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    print("hii");

    val spark=SparkSession.builder().master("local[*]").appName("setup").getOrCreate();


//    import spark.implicits._;
//
//    val data = Seq((1, "Hardik"), (2, "Gaur"))
//
//    val df = data.toDF()
//
//    df.show()
//
//    val df = spark.createDataFrame(
//      List(("Scala", 25000), ("Spark", 35000), ("PHP", 21000)))
////    df.show()
//
//    df.createOrReplaceTempView("data")
//
//    val df2 = spark.sql("select _1 from data")
//
//    df2.show()

//    val d = spark.sparkContext.textFile("/Users/hardikgaur/Desktop/sample.txt")
//
//    d.foreach(println)


//    val rdd = spark.sparkContext.parallelize(Array(1,2,3,4,5,6,7,8))
//
//    val res = rdd.collect()
//
//
//
//    println(res.mkString(","))
//    println(rdd.getNumPartitions)
//
//    val emp = spark.sparkContext.parallelize(Seq.empty[Int])
//
//    println(emp.collect().mkString(","))


//    val rdd = spark.sparkContext.textFile("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/TextFiles/User_Data.txt").map(_.split(","))

//    rdd.collect().foreach((f)=> println(f.mkString("[", ",","]")))


    // to remove the first row , containing the attributes name
//    val rdd = spark.sparkContext.textFile("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/TextFiles/User_Data.txt").mapPartitionsWithIndex((idx, itr)=> if(idx==0) itr.drop(1) else itr)
//
//    rdd.foreach(println)
//
//    // filter data
//    val res = rdd.collect.filter((row)=>{
//
//      val age = row.split(",")(2).toInt
//      age >= 30 && age <=40
//
//    });
//
//    res.foreach(println)
//


    //  spark actions



//    val rdd = spark.sparkContext.parallelize(Seq(1,2,3,4,5,6,7,8))
    val rdd = spark.sparkContext.textFile("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/TextFiles/User_Data.txt").mapPartitionsWithIndex((idx, itr)=> if(idx==0) itr.drop(1) else itr)
    // 1. collect()  : collects the all the elements of the rdd
//     rdd.collect.foreach(println)

    // 2. count()  : count the elements in the rdd
//    println(rdd.count())

    // 3. first() : get the first element of the rdd
//    println(rdd.first())


    // 4. take(n) : get the first n elements of the rdd as an array
//    rdd.take(8).foreach(println)

    // 5. reduce() : reduces the elements of the rdd using the provided binary operator function func.
    // finding the sum of age of first three rows , using reduce

    /* way 1*/
//    val totAge = rdd.take(3).reduce((a, b)=>{
//
//      val sp1 = a.split(",")
//      val sp2 = b.split(",")
//      val age1 = sp1(2).toInt
//      val age2 = sp2(2).toInt
//
//      sp1(2) = ((age1 + age2)).toString
//
//      sp1.mkString(",")  // we need to the exact same format (as string)
//    }).split(",")(2).toInt
//
//    println(totAge)

    /* way 2 */
//     val totAge = rdd.take(3).map(_.split(",")(2).toInt).reduce(_ + _)

//    println(totAge)


    // 6. takeOrdered(n) : returns the first n elements from rdd or dataframe after sorting in ascending order

    // finding  10 smallest ages
//     val res  = rdd.map(_.split(",")(2).toInt).takeOrdered(10)
//      res.foreach((f)=> print(f + " "))
//    println

    // 7. saveAsTextFile(path) : saves the content of the rdd or dataframe to a text file in the specified path. The output is partitioned across multiple files.

//    rdd.saveAsTextFile("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/savedFiles")


    // 8.  countByKey() : for an RDD  of key-value pairs, this action counts the number of element for each key

    val rdd2 = spark.sparkContext.parallelize(Seq((1, "Hardik"), (2, "hey"), (1 , "Bye"), (1, "Yo")))
//
//
//    println(rdd2.countByKey)

    // 9. takeSample(withReplacement, num, [seed]) : returns a random smaple of num elements from the dataset, with or without replacement. Optionally,you can specify a seed for deterministic value

//  a .  Sampling with Replacement (withReplacement = true):
// Each element of the RDD can be selected multiple times in the result. The chance of selecting any element is independent of the other selections.

//  b.   Sampling without Replacement (withReplacement = false):
//
//      Once an element is chosen, it cannot be selected again. The result is guaranteed to have distinct elements, up to the size of the RDD or num, whichever is smaller.

//  c.  Random Seed (seed):
//
//      If you want to generate reproducible results (i.e., the same sample every time), you can specify a seed value. This ensures that the random sampling is deterministic.

//  d.  Parallelism:
//
//      Since Spark is a distributed system, the sampling is performed in parallel across multiple partitions, and then the results are combined into a single array returned to the driver program.

//    val sample = rdd.takeSample(withReplacement = false, 10)
//    sample.foreach(println)


    // 10. saveAsSequenceFile(path) : saves the contents of the rdd as a hadoop SequenceFile at the specified path. SequeneFiles are used to store key-value pairs

    val rdd3 = spark.sparkContext.parallelize(Seq(( 1, "B" ), (2 , "B"), (3, "C"), (1 , "A")))
//    rdd3.saveAsSequenceFile("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/sequenceFiles")


    // 11. countByValue() : returns a map with the count of each unique value in the rdd (for array, not for key value pairs)

    val rdd4 = spark.sparkContext.parallelize(Seq(1,1,1,4,5,3,3,4,5,3,2))
//
//    val res = rdd3.countByValue()
//    println(res((1, "A")))
//    println(rdd4.countByValue)


    // 12.  max() : returns the maximum element in the rdd

//    println(rdd4.max)

    // find the rows with maximum ages in rdd1
//    val maxAge = rdd.collect.map(_.split(",")(2).toInt).max
//    val rowsWithMaxAge = rdd.filter(_.split(",")(2).toInt == maxAge)
//
//    rowsWithMaxAge.collect.foreach(println)

    // 13. min() : returns the minimum element in the rdd

//    println(rdd4.min)


    // 14. aggregate(zeroValue, seqOp, combOp) : Aggregates the elements of the RDD or DataFrame, using an
    // initial zero value (zeroValue) and two functions: seqOp (for element-wise aggregation) and combOp
    // (for merging aggregations from different partitions)

    // get the sum of elements and counts of element in one go
//    val zeroValue = (0,0)
//    val seqOp  = (acc : (Int, Int), value: Int) => (acc._1 + value ,  acc._2 + 1)
//
//    val combOp = (acc1 : (Int, Int), acc2 : (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
//
//
//    val (sum, count) = rdd4.collect.aggregate(zeroValue)(seqOp, combOp)
//
//    println(sum, count)


      // 15. fold() : similar to reduce , but with a zero value. The zero value is applied at the start of the operation ensuring that the dataset can be empty
//    val red = rdd4.fold(0)(_ + _)
//    println(red)

    //  16. foreachPartition() : applies a function func to each partition of the rdd. This can be more efficient than foreach() when you want to perform an action on a partition basis , such a writing to a database

//    val rdd5 = spark.sparkContext.parallelize(Seq(1,2,3,4,5,6,7,8,9,10),3)
//
//    rdd5.foreachPartition(par=> println(par.mkString(" ")))

    // 17. takeOrdered(n,. ordering) : return the first n element based on a custom ordering


//    val res = rdd4.takeOrdered(3)(Ordering[Int].reverse)  // sort in descending
//
//    println(res.mkString(" "))

    /// 22. lookup(key) : for key-value rdds, this action returns all values associated with a given key


    val valuesForA = rdd3.lookup(1)
    println(valuesForA.mkString(" "))











  }



}