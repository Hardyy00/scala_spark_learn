package string

object Main {

  val num1 = 89;
  val num2 = 100.23;
  def main(args : Array[String]) : Unit = {

    val name = "Hardik"
    val age = 18


    // string interpolation
    println(s"$name is $age years old") // not type safe
    println(f"$name%s is $age%d years old") // type safe

    println(s"hello \nworld")
    println(raw"hello world \nworld")


    val s = "hardik"
    println(s.length())


    println(s.concat(" gaur"))
    println(s)
    // string formatting can be done in 2 ways using format and printf

    printf("(%d -- %f -- %s)", num1, num2, s)
    println("(%d -- %f -- %s)".format(num1, num2, s))


  }

}
