object Main {
  def main(args: Array[String]): Unit = {

    val numList= List(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)

    for ( x <- numList)
      {
        if (x%3 ==0)

println(x)
      }

    }
  }