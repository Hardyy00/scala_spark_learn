//import scala.collection.{Map, BitSet};
//
//class Point(val a : Int, val b : Int){
//
//    var x : Int = a;
//    var y : Int = b;
//
//    def move(a : Int,b : Int): Unit = {
//
//      println(x + a + " " + (y + b));
//    }
//
//}
//
//class Location(override val a : Int,override  val b : Int, val c : Int ) extends Point(a, b){
//
//  var z : Int = c;
//
//
//  override def move(a : Int , b : Int): Unit = {
//
//    println(a + " " + b)
//  }
//}
//object Main {
//  def main(args: Array[String]): Unit = {
//    var a: Int = 1; // mutable
//    val b: Int = 2; // immutable
//
//    println(a + " " + b);
//    a = 3;
//
//    var c: String = "Hardik Gaur";
//    println(a + " " + b + " " + c)
//
//
//    var (v1: Int, v2: String) = (45, "hardik");
//
//    println(v1, v2)
//
//
//    val obj: Point = new Point(1, 2)
//
//    obj.move(10, 10)
//
//    val loc : Location = new Location(10,10,20);
//
//    loc.move(1,1)
//
//
//
//
//  }
//}