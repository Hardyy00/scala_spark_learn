package higher_order_functions

import java.util.Date

object Main {

  def math(x : Double , y : Double,f : (Double , Double)=> Double) : Double = f(x,y)

  def log(date : Date, message : String) : Unit = {

    println(date + " " + message)
  }

  def main(args : Array[String]) ={

    println(math(1,2,(x,y)=> x + y))

    println(math(1,2, (x,y)=> x min y))

    println(math(1,2,_+_))  /// using wildcard


    // partially applied function

    val sum = (a : Int, b : Int ,c : Int) => a + b + c;

    val f = sum(10,20, _ : Int) // _ acts like a wildcard

    println(f(10))


    val date= new Date()
    val newLog = log(date, _ : String)

    newLog("Message 1")



  }


}
