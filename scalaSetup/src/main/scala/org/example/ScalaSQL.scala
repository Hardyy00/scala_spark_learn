package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{array_contains, avg, col, count, lit, struct, sum, udf}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

 class Util extends  Serializable{

  def combine(fName : String, mName : String, lName : String) : String ={

      return fName + "," + mName + "," + lName
  }
}

object ScalaSQL extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().master("local[*]").appName("SQL").getOrCreate()
  import spark.implicits._;

   val sc = spark.sparkContext;

  ///////////////             creating dataframes               ////////////////

//  val columns = Seq("language","users_count")
//  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
//
//
//  //  1. dataframe from Seq or list
//  val rdd= sc.parallelize(data)
//  val df = rdd.toDF(columns : _*)
//
//  df.printSchema()
//
//  df.show()
//
//
//
//  // 2. dataframe from rdd
//  val dfFromRDD = spark.createDataFrame(rdd).toDF(columns : _*)
//
//  dfFromRDD.show()
//
//  // 3. dataframe from Row RDD i.e. RDD[Row] instead of RDD[T] . for that we need to create a row rdd as well as a schema for the new dataframe
//
//  val rowRDD = rdd.map((row)=> Row(row._1, row._2))
//
//  val schema = StructType(Array(StructField("language", StringType, nullable =  false), StructField("users_count", StringType, nullable=false)))
//
//  val dfFromRowRdd = spark.createDataFrame(rowRDD, schema)
//
//  dfFromRowRdd.show()
//
//
//  // 4. using toDF() on list or Seq collection
//
//  val dfFromSeqOrList = data.toDF(columns : _*)
//  dfFromSeqOrList.show()
//
//  // 5. create dataframe from csv
//
//  // here the problem is we are not using the first row as the headers, but we want to do that
//
////  val dfFromCsv = spark.read.csv("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/csv/users.csv");
////  dfFromCsv.show(false)
//
//  // to use the first row as the header , we have to manually set that as the first row
//  val dfFromCsv = spark.read.option("header", value = true).csv("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/csv/users.csv")  // with this we are using the first row as header
//  dfFromCsv.show()

  // 6. create dataframe from json

//  val dfFromJson= spark.read.json("/Users/hardikgaur/IdeaProjects/scalaSetup/src/main/scala/json/sample2.json")
//  dfFromJson.show()






  ////////////////////////////             where function           ///////////////////////////////


//  val arrayStructuredData = Seq(
//    Row(Row("James", "", "Smith"), List("Java","Scala", "C++"), "OH", "M"),
//    Row(Row("Anna", "Rose", ""), List("Spark", "Java", "C++"), "NY", "F"),
//    Row(Row("Julia", "", "Williams"), List("CSharp", "VB"), "OH", "F"),
//      Row(Row("Maria", "Anne", "Jones"), List("CSharp", "VB"), "NY", "M"),
//      Row(Row("Jen", "Mary", "Brown"), List("CSharp", "VB"), "NY", "M"),
//      Row(Row("Mike", "Mary", "Williams"), List("Python", "VB"), "OH", "M")
//  )
//
//  val arrayStructuesSchema = StructType(
//    Seq(
//      StructField("name", StructType(
//        Seq(
//          StructField("firstName", StringType, nullable = false),
//          StructField("middleName", StringType, nullable = true),
//          StructField("lastName", StringType, nullable = false)
//        )
//      )
//      ),
//
//      StructField("languages", ArrayType(StringType), nullable = true),
//
//      StructField("state", StringType, nullable= false),
//      StructField("gender", StringType, nullable = false)
//    )
//  )
//
//  val df = spark.createDataFrame(sc.parallelize(arrayStructuredData), arrayStructuesSchema)
//
//
//  println(df.printSchema())
//  df.show()
//
//
//  // where() syntaxes :
//
////  1) where(condition: Column): Dataset[T]
////  2) where(conditionExpr: String): Dataset[T] //using SQL expression
////  3) where(func: T => Boolean): Dataset[T]
////  4) where(func: FilterFunction[T]): Dataset[T]
//
//
//  // 1. with column condition
//
//  df.where(df("state") === "OH").show()
//
//  // 2. where() with sql expression
//
//  df.where("state == 'OH' " ).show(false)
//
//  // 3. selecting on an array column
//
//  df.where(array_contains(df("languages"), "Scala")).show()
//
//  // 4. selecting a nested struct columns
//
//  df.where(df("name.lastName") === "Williams").show()



  ///////////////////////               withColumn()               //////////////////////////


//    val data  = Seq(Row(Row("James;","","Smith"),"36636","M",3000),
//    Row(Row("Michael","Rose",""),"40288","M",4000),
//    Row(Row("Robert","","Williams"),"42114","M",4000),
//    Row(Row("Maria","Anne","Jones"),"39192","F",4000),
//    Row(Row("Jen","Mary","Brown"),"","F",-1)
//  )
//
//
//  val dataSchema = StructType(
//    Seq(
//
//      StructField("name", StructType(
//        Seq(
//          StructField("firstName", StringType, nullable=false),
//          StructField("middleName",StringType, nullable = true),
//          StructField("lastName", StringType, nullable=false)
//        )
//
//      )
//      ),
//
//
//        StructField("dob", StringType, nullable=false),
//        StructField("gender", StringType, nullable=false),
//      StructField("salary", IntegerType, nullable=false)
//
//
//
//    )
//  )
//
//  // add a new column (lit(value) : is used to add a value or a literal
//
//  val df = spark.createDataFrame(sc.parallelize(data), dataSchema)
//  df.withColumn("country", lit("USA")).show() //  ADD A NEW COLUMN WHOSE VALUE IS 'USA'
//
//  // change value of an existing column
//
//  df.withColumn("salary", df("salary")*0).show()
//
//  // derive a new column from an existing column
//
//  df.withColumn("new column", df("salary") * 2).show()
//
//  // change column data type
//
//  df.withColumn("salary", df("salary").cast("String")).show()
//
//  // add, replace or update multiple columns
//
//  df.createOrReplaceTempView("PERSON")
//
//  val modifiedDF = spark.sql("select salary * 100 as salary, salary * -1 as copiedColumn, 'USA' as country from PERSON")
//
//  modifiedDF.show()
//
//  // rename column name : using withColumnRenamed()
//
//  df.withColumnRenamed("gender","sex").show()
//
//  // drop a column : using .drop() method
//
//  df.drop("gender").show()
//
//
//  // drop multiple columsn
//
//
//  df.drop("gender", "name").show()
//
//  // or
//
//  val cols = Seq("gender", "name")
//
//  df.drop(cols : _*).show()






  ////////////////////////////////                distinct()                    ///////////////////////////



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
//  val distinctDf = df.distinct()
//
//  distinctDf.show()
//  println(distinctDf.count())
//
//
//  // spark distinct of multiple rows
//
//  // Spark doesn’t have a distinct method that takes columns that should run distinct on however, Spark provides another signature of dropDuplicates() function which takes multiple columns to eliminate duplicates.
//  //Note that calling dropDuplicates() on DataFrame returns a new DataFrame with duplicate rows removed.
//
//
//  val dropDupDf = df.dropDuplicates("department", "salary")  // remove duplicates having same 'department' and 'salary'
//  dropDupDf.show()
//  println(dropDupDf.count())






  /////////////////////////////                       groupBy()              //////////////////////////////

//  val simpleData = Seq(("James","Sales","NY",90000,34,10000),
//    ("Michael","Sales","NY",86000,56,20000),
//    ("Robert","Sales","CA",81000,30,23000),
//    ("Maria","Finance","CA",90000,24,23000),
//    ("Raman","Finance","CA",99000,40,24000),
//    ("Scott","Finance","NY",83000,36,19000),
//    ("Jen","Finance","NY",79000,53,15000),
//    ("Jeff","Marketing","CA",80000,25,18000),
//    ("Kumar","Marketing","NY",91000,50,21000)
//  )
//  val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
//  df.show()
//
//
//  // groupBy and aggregate on dataframe columns
//
//
//  df.groupBy("department").sum("salary").show(false)  // group by sum , and apply sum() on salary for each department (or group)
//
//  df.groupBy("department").count().show(false)
//
//  df.groupBy("department").min("salary").show(false)
//
//  df.groupBy("department").max("salary").show(false)
//
//  df.groupBy("department").mean("salary").show(false)
//
//  // groupBy and aggregate on multiple dataframe columns
//
//  df.groupBy("department", "state").sum("salary", "bonus").show(false)
//
//
//  // Using agg() aggregate function we can calculate many aggregations at a time on a single statement using Spark SQL aggregate functions sum(), avg(), min(), max() mean() e.t.c
//
//
//  df.groupBy("department").agg(sum("salary").as("sum_salary"), count("salary").as("count") , avg("salary").as("avg salary"),sum("bonus").as("bonus_sum"), avg("bonus").as("bonus_average") ).show()
//
//
//  // using filter on aggregate data
//
//  df.groupBy("department").agg(sum("salary").as("salary sum"), count("salary").as("count")).where(col("salary sum") >= 257000).show()






  ////////////////////////////////////                                   join()                       /////////////////////////////////////////


//  val emp = Seq((1,"Smith",-1,"2018","10","M",3000),
//    (2,"Rose",1,"2010","20","M",4000),
//    (3,"Williams",1,"2010","10","M",1000),
//    (4,"Jones",2,"2005","10","F",2000),
//    (5,"Brown",2,"2010","40","",-1),
//    (6,"Brown",2,"2010","50","",-1)
//  )
//  val empColumns = Seq("emp_id","name","superior_emp_id","year_joined",
//    "emp_dept_id","gender","salary")
//  val empDF = emp.toDF(empColumns:_*).withColumn("emp_dept_id", col("emp_dept_id").cast("Integer"))
//  empDF.show(false)
//
//  val dept = Seq(("Finance",10),
//    ("Marketing",20),
//    ("Sales",30),
//    ("IT",40)
//  )
//
//  val deptColumns = Seq("dept_name","dept_id")
//  val deptDF = dept.toDF(deptColumns:_*)
//
//  empDF.printSchema()
//
//  deptDF.show()
//
//
//
//
//  /// inner join
//
//  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "inner").select("emp_id", "name","dept_id", "salary", "dept_name").show(false)
//
//
//
//  // full outer join
//
//  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "outer").show() // or "full", "fullouter"
//
//  // left outer join
//
//  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "left").show()  // or "leftouter"
//
//  // right outer join
//
//  empDF.join(deptDF,empDF("emp_dept_id") === deptDF("dept_id"), "right").show()  // or "rightouter"
//
//  // left semi joni
//
//  /* Spark Left Semi join is similar to inner join difference being leftsemi join returns all columns
//  from the left DataFrame/Dataset and ignores all columns from the right dataset. In other words, this
//  join returns columns from the only left dataset for the records match in the right dataset on join
//  expression, records not matched on join expression are ignored from both left and right datasets. */
//
//  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftsemi").show(false)
//
//  // left anti join
//
//  /* Left Anti join does the exact opposite of the Spark leftsemi join, leftanti join returns only columns from the left DataFrame/Dataset for non-matched records. */
//
//  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftanti").show()





  ///////////////////////                     map()  vs mapPartitions()                ///////////////////////

//  val structureData = Seq(
//    Row("James","","Smith","36636","NewYork",3100),
//    Row("Michael","Rose","","40288","California",4300),
//    Row("Robert","","Williams","42114","Florida",1400),
//    Row("Maria","Anne","Jones","39192","Florida",5500),
//    Row("Jen","Mary","Brown","34561","NewYork",3000)
//  )
//
//  val schema = StructType(StructField("firstName", StringType) :: StructField("lastName", StringType) :: StructField("middleName", StringType) :: StructField("id", StringType) :: StructField("location", StringType) :: StructField("salary", IntegerType) :: Nil)
//
//  val df = spark.createDataFrame(sc.parallelize(structureData),schema)
//  df.printSchema()
//  df.show()
//
//  var df2 = df.map((row)=>{
//
//    val util = new Util();
//
//
//    val fullName = util.combine(row.getString(0) , row.getString(1), row.getString(2))
//
//    (fullName, row.getString(3), row.getInt(5))
//  }).toDF("fullName", "id", "salary")
//
//  df2.show(false)

  // Since map transformations execute on worker nodes, we have initialized and create
  // an object of the Util class inside the map() function and the initialization happens
  // for every row in a DataFrame. This causes performance issues when you have heavily weighted initializations.



  ////////      mapPartitions()


  /**  Spark mapPartitions() provides a facility to do heavy initializations (for example Database connection) once for each partition instead of doing it on every DataFrame row. This helps the performance of the job when you dealing with heavy-weighted initialization on larger datasets. **/

//  val df4 = df.mapPartitions((itr)=>{
//
//    val util = new Util() // heavy initialization is done at the beginning for every partition
//
//    val res = itr.map((row)=>{
//
//      val fullName = util.combine(row.getString(0) , row.getString(1), row.getString(2))
//
//      (fullName, row.getString(3), row.getInt(5))
//    })
//
//    res
//  }).toDF("fullName", "id", "salary")
//
//  df4.show()


  /////////////////////                    pivot()            /////////////////////////////


  /*   Spark pivot() function is used to pivot/rotate the data from one DataFrame/Dataset
  column into multiple columns (transform row to column) and unpivot is used to transform
  it back (transform columns to rows).*/

//  val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
//    ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
//    ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
//    ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))
//
//
//
//  val df = data.toDF("Product", "Amount", "Country")
//
//  df.show()
//
//
//  val pivotDF = df.groupBy("Product").pivot("country").sum("amount")
//  pivotDF.show()


  ////////////////////////////                      UDF() : user defined functions                  //////////////////////

  val cols = Seq("Seqno", "Quote")
  val data = Seq(("1", "Be the change that you wish to see in the world"), ("2", "Everyone thinks of changing the world, but no one thinks of changing himself" ), ("3", "The purpose of our lives is to be happy."))

  val df = data.toDF(cols : _*)

  df.show(false)


  // convert first letter of every words in a sentence to capital
  val convertCase = (str : String)=>{

    val arr = str.split(" ")
    arr.map(f => f.substring(0,1).toUpperCase + f.substring(1)).mkString(" ")
  }

  val convertUDF = udf(convertCase)  // convert function to udf

  // using it on a dataframe

  df.select(col("Seqno"), convertUDF(col("Quote")).as("Quote") ).show(false)


  // registering spark UDF to use it on SQL

  spark.udf.register("convertUDF", convertUDF)

  df.createOrReplaceTempView("Quotes")

   spark.sql("select Seqno, convertUDF(quote) as Quote from Quotes").show(false)






}
