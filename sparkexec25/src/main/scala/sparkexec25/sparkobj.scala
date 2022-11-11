package sparkexec25

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


object sparkobj {
  
  case class schema(txnno:String, category:String, product:String)
   
  def main(args:Array[String]):Unit={
    
    val conf=new SparkConf().setAppName("first").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val spark = SparkSession
  .builder()
  .getOrCreate()
  import spark.implicits._
    sc.setLogLevel("ERROR")
    
    val data=sc.textFile("file:///C:/data/txnsmall.txt")
    data.foreach(println)
   
    println("====================filetered data====================")
    
    val gymdata=data.filter(x=>x.contains("Gymastics"))
  
    
    gymdata.foreach(println)
   
    println ("Schema RDD")
    
      
    val splitdata=data.map(x=>x.split(",")) //split data
    val schemardd =splitdata .map(x=> schema(x(0),x(1),x(2))) //imposing schema to data
    val filterschema=schemardd.filter(x => x.product.contains("Gymnastics")
      && x.txnno.contains("67")) 
   
      val df =filterschema.toDF()
      df.show()
  }
}