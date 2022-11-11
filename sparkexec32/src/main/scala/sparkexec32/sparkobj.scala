package sparkexec32

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object sparkobj {

	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val spark =  SparkSession.builder().getOrCreate()
					import spark.implicits._

					println("======handson 1=============")
					val donutdf= spark.read.format("json").option("multiline","true").load("file:///c:/data/complexdata/donut.json")
					donutdf.printSchema()
					//donutdf.show()

					val flattendonutdf= donutdf.select(col("id"),
							col("type"),
							col("name"),
							col("image.url").as("iurl"),
							col("image.width").as("iwidth"),
							col("image.height").as("iheight"),
							col("thumbnail.url").as("turl"),
							col("thumbnail.width").as("twidth"),
							col("thumbnail.height").as("theight")

							)
					flattendonutdf.show()
					
					println("======handson 2=============")
					val zeyocdf=spark.read.format("json").option("multiline","true").load("file:///c:/data/complexdata/zeyoc.json")
					zeyocdf.printSchema()
					
					val flattenzeyocdf= zeyocdf.select(col("Students"),
							col("address.permanentAddress").as("permanentAddress"),
							col("address.temporaryAddress").as("temporaryAddress"),
							col("orgname"),
							col("trainer"))
							val finalflattenzeyodf= flattenzeyocdf.withColumn("Students",explode($"Students"))
							finalflattenzeyodf.show()   
							
							println("======handson 3=============")		
							val flattenzeyocdf1= zeyocdf.select(col("Students"),
							col("address.*"),
							col("orgname"),
							col("trainer"))
							val finalflattenzeyodf1= flattenzeyocdf.withColumn("Students",explode($"Students"))
							finalflattenzeyodf1.show() 
						 
	}		
	}

