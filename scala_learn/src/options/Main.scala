package options

object Main {



  val lst = List(1,2,3)
  val map = Map(1-> "hjarde",2 -> "dhad", 4 -> "hdsa")
  val opt : Option[Int] = Some(5)

  def main(args : Array[String]) ={
    // options is a container that can give two values
    //  instance of Some or instance of None
    // (Some, None)


    val res = lst.find(_ > 1)


    // Some.get gives the value inside the Some
    println(res.get + 4)

    println(map.get(2))
    println(map.getOrElse(2, "No such key"))


    println(opt)

    println(opt.isEmpty)
  }

}
