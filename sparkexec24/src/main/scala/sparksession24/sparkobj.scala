package sparksession24

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
//import org.apache.spark._

object sparkobj {
  
  def main(args:Array[String]):Unit=
  {
    
    val conf=new SparkConf().setAppName("first").setMaster("local[*]")
    val sc= new SparkContext(conf)
    
    sc.setLogLevel("ERROR")
    val data=sc.textFile("file:///C:/data/regiondata.txt")
    
    
    val fildata=data.flatMap(x => x.split("~"))
    
    val filterstates=fildata.filter(x => x.contains("State"))
    val filtercities=fildata.filter(x => x.contains("City"))
    
    val finalstate=filterstates.map(x => x.replace("State->",""))
    val finalcity=filtercities.map(x => x.replace("City->",""))
   println("========State list==========")
    finalstate.foreach(println)
    
      println("==========City list========")
    finalcity.foreach(println)
    
  }
  
  
  
}