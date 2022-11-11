package greaterspark

object list_22 {

  def main(args: Array[String]): Unit = {

    println("=========started============")

    val a = 4
    println(a)
    val b = "zeyobron"
    println(b)
    val listint = List(1, 2, 3, 4)
    println(listint)
    listint.foreach(println)
    println("=========greater than 2============")

    val newlist = listint.filter(x => (x > 2))
    newlist.foreach(println)
    println("=========muliply by 2============")

    val mullist = listint.map(x => (x * 2))
    mullist.foreach(println)

    println("=========TASK1============")
    val listint2 = List(10, 20, 30, 40, 50)
    val newlist2 = listint2.filter(x => x < 45)
    println("=========filter list============")
    newlist2.foreach(println)

    println("=========TASK2=========")
    val newlist3 = listint2.map(x => x / 5)
    println("=========Div by 5============")

    newlist3.foreach(println)

  }
}