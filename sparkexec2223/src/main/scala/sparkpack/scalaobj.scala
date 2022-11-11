package sparkpack

object scalaobj {
  
  
  def main(args:Array[String]):Unit=
    
  {
    println("===started===== ")
    val a=2
    println(a)
    val b= "zeyobron"
    println(b)
    
    println("raw list")
    val listint = List(1,2,3,4)
    listint.foreach(println)
  
    val liststr = List("zeyo","bron","zeyobron","bigdata")
    liststr.foreach(println)
    
        println("filter list")
    val proclist = listint .filter(x => x > 2)
    val proclist1 =listint.filter(x => x < 4)
    proclist.foreach(println)
    proclist1.foreach(println)
    
    println("muliplied the list of less than 4")
    val mullist =proclist1.map( x => x *2)
    mullist.foreach(println)
     
    println("Add 10 to multiplied list")
      val addlist = mullist.map (x => x+10)
      addlist.foreach(println)
    
      println("fetch zeyo\n")
      val fillstr= liststr.filter(x => x.contains("zeyo"))
      fillstr.foreach(println)
     
      
      println("map list")
      /*val attach = ",Ko" */
      val maplist = fillstr.map(x => x + ",ko")
      maplist.foreach(println)
      
      println("replace list")
      
      val replacelist = liststr.map(x => x.replace("zeyo","ZEYO"))
          replacelist.foreach(println)
      
          
          println("flatten")
          
          val liststr2=List("A~B","C~D","E~F")
          /* not usual map , its going to be flatMap*/
          
          val flatlist=liststr2.flatMap(x => x.split("~"))
              flatlist.foreach(println)
  
              
              println("usecase1")
              val list1 = List("azeyobron~analytics","Zeyo~bigdata",
                  "hive~zeyo")
                  
                  val flattenlist1= list1.flatMap(x => x.split("~"))
                  flattenlist1.foreach(println)
                  
                  val filflat = flattenlist1.filter(x =>x.contains("zeyo"))
                  filflat.foreach(println)
                  
                  val replflat= flattenlist1.map(x => x.replace("Zeyo","Ko"))
                 replflat.foreach(println)
                 
                 val suflist= replflat.map(x => x + ",var") 
                 suflist.foreach(println)
                  
                 
                 println("from local file")
                 
                
                  
  }
  
}


