package reduce_fold_scan

// reduce, fold or scan (left/right)

// reduceleft, reduceRight, foldLeft, foldRight, scanLeft ,scanRight

object Main {

  val lst = List(1,2,3,4,5,6)
  val lst2 = List("A", "B", "C")
  def main(args : Array[String]): Unit = {

    println(lst.reduce( _ + _))
    println(lst.reduce((x,y)=>{

      println(x + " " + y)
      x + y
    }))


//    println(lst.reduceLeft(_ - _))
//    println(lst.reduceRight(_ - _ ))

    println(lst2.reduceRight((x,y)=>{ println(x + " " + y); x + y}))
    println(lst2.reduceLeft((x,y)=>{ println(x + " " + y); x + y}))


    println(lst.fold(100)(_ + _))

    println(lst2.scan("Z")(_ + _))

  }

}
