package match_expression

object Main {

  def main(args : Array[String]) : Unit = {

      val age = 18;


      // _ -> default
      age match {
        case 20 => println(age)
        case _ => println("no")
      }

      val x = age match {
        case 20 => age
        case _ => "no"
      }

    println(x)


    val i = 10;


    // | -> OR
    i match {

      case 1 | 3 | 5 | 7 | 9 => println("Odd")
      case 2 | 4 | 6 | 8 | 10 => println("Even")
    }
  }

}
