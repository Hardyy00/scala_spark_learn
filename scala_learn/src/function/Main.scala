package function

object Main {

  object Math{

    def add(x : Int, y : Int)= x + y

    def square(x : Int) = x * x

    def **(x : Int, y : Int) = x * y
  }

  def default(x : Int = 20 ,y : Int = 30) = x + y


  def main(args : Array[String]) : Unit = {

    // last line of a function is a return statement
    println("hardik")

//    println(add(1,2))

    println(multiply(1,2))

    println(Math.add(1,32))

    println(Math square 4)


    println(default(1))

    print(1,2)

    println(Math ** (2,3))

    var add = (x : Int, y : Int) => x + y


    println(add(100,200))

  }

  // functions that doesn't return anything
  def print(x : Int , y : Int) : Unit = {

    println(x ,y)
  }

  def add(a : Int, b : Int) : Int = {

     a + b
  }
  def multiply(a : Int, b : Int) = a *b;

}
