package list

object Main {

  val lst : List[Int] =  List(1,2,3,4,5)
  def main(args : Array[String]): Unit = {

//       in scala, list are immutable and arrays are mutable

    println(lst)
    println(lst(0))

    // 2 building blocks of list
    // 1 -> nil
    // 2 -> cons (::)

    // prepend element at the beginning of a list (temporary)

    println(100 :: lst)

    // Nil is an empty list

    println(1::5::6::Nil)

    println(lst.reverse)


    println(List.fill(5)(0))

    lst.foreach(println)
    var sum : Int = 0;

    lst.foreach(sum += _)

    println(sum)






  }

}
