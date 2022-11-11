package greaterspark
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._

object agggroup30 {
  
  def main(args:Array[String]):Unit={
     val conf=new SparkConf().setMaster("local[*]").setAppName("appgroup")
  val sc= new SparkContext(conf)
  sc.setLogLevel("ERROR")
  val spark= SparkSession.builder().getOrCreate()
  import spark.implicits._
  

  
    println("===================task1=================")
   
   val data2= spark.read.format("csv").option("header","true").load("file:///c:/data/zeyodata1.txt")
   
  val procdata2= data2.agg(sum("amount").alias("total"),count("product").alias("procount"))
   
   procdata2.show()
   
      println("===================task2=================")
   val data1=spark.read.format("csv").option("delimiter","~").option("header","true").load("c:/data/txns_head")
  data1.printSchema()
  
  val procdata1=data1.groupBy("category").agg(sum("amount").alias("total")).withColumnRenamed("category","ty")
procdata1.show()
}
}