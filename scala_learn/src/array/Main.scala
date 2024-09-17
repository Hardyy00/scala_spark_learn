package array

import Array._

object Main {
  val arr : Array[Int] = new Array[Int](5);
  val arr2 = new Array[Int](5);
  val arr3 = Array(1,2,3,4,5)


  def main(args : Array[String]): Unit = {

    arr(0) = 10;
    arr(1) = 20;
    arr(2) = 30;
    arr(3) = 40;
    arr(4) = 50;

    for(i <- arr){
      print(i +  " ")
    }
    println()

    val arr4 = concat(arr, arr3)

    for(i <- arr4){
      print(i +  " ")
    }






  }


}
