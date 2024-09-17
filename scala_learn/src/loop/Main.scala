package loop

object Main {

  def main(args : Array[String]): Unit = {

//    var x = 0;
//
//    while(x <4){
//
//      println(x)
//      x +=1;
//    }


    // syntax : for(variable generator range){}
    for(i <- 1 to 5){

      println(i)
    }

//    for(i <- 1.to(5)){
//
//
//    }


    for(i <- 1 until 6){

      println(i)
    }

//    for(i <- 1.until(6)){
//
//
//    }


    // nested loop

    // 1 1
    // 1 2
    // 1 3
    // 2 1

    for(i <- 1 to 5; j <- 1 to 3){

      println(i + " " + j)
    }

    val lst = List(1,7,3,2,5,6,9,4)

    for(i <- lst){

      println(i)
    }

    // we can also apply filters in for loops
    for(i <- lst; if i <=6){

      println(i)
    }

    // for loop as an expression

    val x = for{i <- lst; if i<=6} yield {

      i*i
    }

    println(x)



  }

}
