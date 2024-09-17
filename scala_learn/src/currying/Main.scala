package currying

object Main {

//  def add(x : Int) ={
//
//    (y : Int)=>{
//
//      (z : Int)=>{
//
//        x + y + z;
//      }
//    }
//  }

//  def add(x : Int): Int=>Int=>Int = (y : Int) => (z : Int)=> x + y + z

  // syntax for curried function

  def add(x : Int) (y : Int) (z : Int) = x + y + z
  def main(args : Array[String]): Unit = {

    println(add(1)(2)(3))

//    val sum = add(1)

//    println(sum(3)(4))



    println(add(1)(2)(8))


   val sum = add(10)_  // using wildcard
   println(sum(20)(30))
  }
}
