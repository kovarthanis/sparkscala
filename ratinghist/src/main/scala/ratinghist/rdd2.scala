package ratinghist

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql._

object rdd2 {

  def findout(splitdata:String)={
    val finsplit= splitdata.split(",")
    val age : Int=finsplit(2).toInt
    val frineds : Int=finsplit(3).toInt
    (age,frineds)
  }
  
  
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("average friends by age").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()

    val data = sc.textFile("file:///C:/SparkScala3/SparkScala3/fakefriends.csv")
   // data.foreach(println)
    
    val splitdata=data.map(findout)
    val totalbyage= splitdata.mapValues(x=>(x,1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
    totalbyage.foreach(println)
   
val avgbyage=totalbyage.mapValues(x=> (x._1 / x._2))
val resu=avgbyage.collect()

resu.foreach(println)
  }
}