package spark28

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._



object sparkobj {


	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc =new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._
					println("=======================TASK1======================")

					val readxml=spark.read.format("com.databricks.spark.xml").option("rowTag","POSLog").load("c:/data/transactions.xml")
					readxml.show()
					readxml.printSchema()

					println("=======================TASK2======================")
					val readcsv = spark.read.format("csv").option("header","true").load("c:/data/usdata.csv")
					val readcsvdf=readcsv.select("first_name","last_name").filter("state='LA'")
					readcsvdf.show()
	}
}
