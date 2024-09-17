package set

object Main {

  val set : Set[Int] = Set(1,2,3,3,3,4,5)  // immutable set
  val set2  =  scala.collection.mutable.Set(1,2,2,3,4,34,3,2)
  val set3  = Set(90,45,64,67,3,3)


  def main(args : Array[String]): Unit = {


    // two types of set : mutable and immutable set
    // by default all sets are immutable

    println(set2 + 10)  // temporary

    // check whether a value is present in a set or not
    println(set2(3))
    println(set2(10))

    println(set ++ set3)  // concat
    println(set &  set3)  // intersect
    println(set intersect set3)
    println(set2(3))
    println(set.max)


    set.foreach(println)




  }

}
