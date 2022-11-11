package spark34

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

					val readjsonarray= spark.read.format("json").option("multiline","true").load("file:///c:/data/complexdata/zeyoc.json")
					readjsonarray.show()
					readjsonarray.printSchema()
					
					println("=========flatten array==========")
					val flattenreadjsonarray = readjsonarray
					.withColumn("Students",explode(col("Students")))
					.withColumn("permanentAddress",col("address.permanentAddress"))
					.withColumn("temporaryAddress",col("address.temporaryAddress"))
					.drop("address")
					flattenreadjsonarray.printSchema()
					
					println("=========Revert to array==========")
					val readjsonarraygen= flattenreadjsonarray.groupBy(col("orgname"), col("trainer"),struct("permanentAddress", "temporaryAddress").as("address")).agg(collect_list("Students").alias("Students"))
					readjsonarraygen.printSchema()
					readjsonarraygen.show()
					
					
	}
}
