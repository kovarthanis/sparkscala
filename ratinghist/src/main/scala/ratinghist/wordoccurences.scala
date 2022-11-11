package ratinghist

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._

object wordoccurences {
  
  def main(args:Array[String]):Unit =
  {
    
    val conf= new SparkConf().setAppName("word count").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark=SparkSession.builder().getOrCreate()
    
    val data=sc.textFile("file:///C:/SparkScala3/SparkScala3/book.txt")
    data.foreach(println)
    
    val flatdata=data.flatMap(x=> x.split("\\W+"))
    
    val res= flatdata.map(x=> (x,1))
    
    val aggres=res.reduceByKey((x,y)=>(x+y))
    
  
    
    val sorted=aggres.map(x=> (x._2,x._1)).sortByKey()
    
        
      val fin=sorted.collect()
      
      fin.foreach(println)
  }
}