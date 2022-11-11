package sparksession24

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object sparkobj1 {
  
  def main(args:Array[String]):Unit=
  {
    val conf=new SparkConf().setAppName("first").setMaster("local[*]")
    val sc= new SparkContext(conf)
        sc.setLogLevel("ERROR")

  val data=sc.textFile("file:///C:/data/usdata.csv")
 
  val fildata=data.filter(x => (x.length())>200)
  val flatdata=fildata.flatMap(x => x.split(","))
  val condata=flatdata.map(x => x + ",Zeyo")
  val repdata=condata.map(x => x.replace("-",""))
   println("=========Display ========")
  repdata.foreach(println)
  
  repdata.coalesce(1).saveAsTextFile("file:///C:/data/processedussdd")
  
  println("=====done written")
  
  
  
  }
}