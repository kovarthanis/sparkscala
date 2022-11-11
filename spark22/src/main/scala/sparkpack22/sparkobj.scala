package sparkpack22

object sparkobj {
  
  def main(args : Array[String]):Unit =
  {
    println("values less than 45")
    val listint =List(10,20,30,40,50)
    val proclist =listint.filter(x => x<45)
    proclist.foreach(println)
    
    
    println("divide by 5")
    val proclist2 =listint.map(x => x/5)
    proclist2.foreach(println)
    
  }
    
    
  
}