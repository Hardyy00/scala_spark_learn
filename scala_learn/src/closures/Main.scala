package closures

// A closure is a function which use one or more variables declared  outside this function

object Main {

  var number = 10;
  val add = (x : Int) => x + number;

  def main(args : Array[String]) {

    println(add(10))

    // impure closures : whenever the datatype of the external variable is var ( value can be changed from inside the closure and outside the closure)
    // pure closure : datatype of external variable is val




  }

}
