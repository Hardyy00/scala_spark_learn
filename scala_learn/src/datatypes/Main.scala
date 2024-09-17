package datatypes

object Main {

  def main(args : Array[String]) : Unit = {

//    println("Hardik")

      var a : Int = 80;  // mutable
      val b : String = "hardik";  // immutable

      var c = true // type  inference


    // we can evaluate an expression to assign a value to a variable
      val x = {

        val a : Int = 200
        val c : Int = 300

         a + c
      }

    val y = {val a : Int = 100; val b : Int = 200; a + b}

    println(x + " "+ y);

    lazy val z = 900;  // it doesn't pre allocate the memory , it only allocates the memory , when we are actually using the variable


    println(z)

  }

}
