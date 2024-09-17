package tuples

object Main {


  val tup = (1,2,"Hello", true)
  val tup2 = new Tuple3(1,2,"hello")
  def main(args : Array[String]): Unit = {


    // tuples can contain heterogeneous data and are immutable

    println(tup)
    println(tup2)

    println(tup._1)


    tup.productIterator.foreach{

      i=> println(i)
    }

    println(1->"Tom") // create tuple





  }



}
