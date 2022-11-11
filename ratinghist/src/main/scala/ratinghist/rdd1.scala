package ratinghist

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._

object rdd1 {
  

  
  
  def main(args: Array[String]):Unit =
  {
    val conf= new SparkConf().setAppName("Rating").setMaster("local[*]")
    val sc= new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark=SparkSession.builder().getOrCreate()
    
    //read from text file as rdd
    val data=sc.textFile("file:///C:/SparkScala3/SparkScala3/ml-100k/u.data")
   // data.foreach(println)
        val linesplit= data.map(x=> x.toString().split("\t")(2))
        //linesplit.foreach(println)
        val countat=linesplit.countByValue()
        val sortedresult=countat.toSeq.sortBy(_._2)
        sortedresult.foreach(println)

 
    
  }
  
  
}