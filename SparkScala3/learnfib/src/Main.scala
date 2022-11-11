object Main {
  def main(args: Array[String]): Unit = {
    var x: Int = 0
    var y: Int = 1

    println(x)
    println(y)

    while(x<=10) {

      fib(x, y)
    }

    cubeIt(x)

    def cubeIt(i: Int): Int =
    {
      i*i*i
    }


    def fib( i: Int, i1: Int) =
    {


      var z = i + i1

      println(z)

      x=i1
      y=z

        }


    }

}