package maps

object Main {

  val map : Map[Int, String] =  Map(1-> "Hardik", 2 -> "Okay")
  val map2 = scala.collection.mutable.Map(1->"Hey", 2->"Nobody")

  def main(args : Array[String]): Unit = {

    println(map(1))  // get value with key 1
    map2.put(3, "so")



    println(map2)

    println(map2.values)
    for(i <- map2){

      println(i)
    }

    map.keys.foreach{
      key => println(key + " " + map(key))
    }

    println(map.contains(200))  // doesn't throw error



  }

}
