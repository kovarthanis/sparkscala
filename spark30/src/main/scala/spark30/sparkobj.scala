package spark30


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object sparkobj {


	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._ 

					val readzeyo = spark.read.format("csv").option("header","true").option("delimiter",",").load("file:///c:/data/zeyodata1.txt")

					//val readzeyo = spark.read.text("file:///c:/data/zeyodata1.txt")
					//readzeyo.show()
					println("====================Task1====================")
					val aggzeyo= readzeyo.groupBy(col("name")).agg((sum("amount").alias("total")),(count(col("product")).alias("procount"))).orderBy(col("total"))

					aggzeyo.show()
					
					
					println("====================Task2====================")
					
					val txnshead = spark.read.format("csv").option("header","true").option("delimiter","~").load("file:///c:/data/txns_head")
					val aggtxnshead= txnshead.filter(col("category") === "Gymnastics").groupBy(col("category"),col("product")).agg(sum(col("amount")).cast("Integer").alias("total"),count(col("spendby")).as("spend_count")).orderBy(col("product") desc)
								//	val aggtxnshead1= txnshead.filter(col("category") === "Gymnastics").groupBy(col("product"),col("spendby")).agg(sum(col("amount")).cast("Integer").alias("total")).orderBy(col("total") asc)

					aggtxnshead.show()
	}

}