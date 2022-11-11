package framessets

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object simple {
  
  case class Person(
    idsy : Int,
    name : String,
    age : Int,
    numfriends : Long
   
    
  ) 
  
  val personschema= new StructType().add("id", IntegerType, nullable=true ).add("name",StringType,nullable=true).add("age",IntegerType,nullable=true).add("numfriends", LongType,nullable=true)
  
  def main(args : Array[String]):Unit={
    
  //  val conf=new SparkConf().setAppName("trial").setMaster("local[*]")
    //val sc=new SparkContext(conf)
  //  sc.setLogLevel("ERROR")
    val spark= SparkSession.builder().appName("trial").master("local[*]").getOrCreate()
    import spark.implicits._
    
    
    val data= spark.read.option("delimiter",",").schema(personschema).option("inferSchema", "true").csv("file:///C:/SparkScala3/SparkScala3/fakefriends.csv")
   val fin= data.select("age","numfriends").groupBy("age").agg(round(avg("numfriends"),2)).sort(avg("numfriends"))
    fin.printSchema()
    fin.show(false)
    
    
  }
  
}