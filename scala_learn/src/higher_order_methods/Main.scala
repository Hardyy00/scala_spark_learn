package higher_order_methods

// map and filter
object Main {



  val lst = List(1,2,3,4,45,5,566,6,54);
  def main(args :Array[String]): Unit = {


    val res = lst.map(_ * 2)

    println(res)

    val s = "hardik"

    // convert to uppercase using map

    println(s.map(_.toUpper))


    println(List(List(1,2,3), List(6,7)).flatten)

    println(lst.flatMap(x=> List(x,x+1)))


    println(lst.filter(_ >50))




  }

}
