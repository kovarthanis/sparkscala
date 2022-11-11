package ratinghist

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._

object mintemp {
  
  def parseit(lines:String)= {
    
    val parsed=lines.split(",")
    val datee= parsed(1)
    val ind= parsed(2)
    val mintemp= parsed(3)
    (datee,ind,mintemp)
      }
  
  def main(args: Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("min").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark=SparkSession.builder().getOrCreate()
    
    val data=sc.textFile("file:///C:/SparkScala3/SparkScala3/1800.csv")
    data.foreach(println)
    
    val parsedata= data.map(parseit)
    val newone= parsedata.filter(x=> x._2=="TMIN")

  }
  
}