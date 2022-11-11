package greaterspark

import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object cdp32 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("cdp")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._
    println("===========tas1==========")
    val data = spark.read.format("json").option("multiline", true).load("file:///C:/data/complexdata/zeyoc.json")
    data.printSchema()
    val procdata = data.withColumn("Students", explode(col("Students")))
    val procdata1 = procdata.select(col("address.permanentAddress"), col("address.temporaryAddress"), col("orgname"), col("trainer"), col("Students"))
    procdata1.show()

    println("===========tas2==========")
    val data1 = spark.read.format("json").option("multiline", true).load("file:///c:/data/complexdata/place.json")
    data1.printSchema()

    val procdata2 = data1.select(col("place"), col("user.address.number").alias("number"), col("user.address.pin").alias("pin"), col("user.address.street").alias("street"), col("user.name"))
    procdata2.show()

    
    println("==============task3=======")
    
    val data2=spark.read.format("json").option("multiline",true).load("file:///c:/data/complexdata/topping.json")
    data2.printSchema()
    
    val procfin= data2.select(col("batters.batter.*"),col("id").as("id2"),col("name"),col("ppu"),col("topping.id").alias("tid"),col("topping.type").alias("tid"),col("type"))
 procfin.show()
  
  }

}