package sparkexec34

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
	
					 val readjson= spark.read.format("json").option("multiline","true").load("file:///c:/data/complexdata/zeyoc34.json")
					readjson.show()
					readjson.printSchema()

					val flattenjson= readjson.					
					withColumn("Students",explode(col("Students")))


					.drop("Streets")

					flattenjson.printSchema()

					val flattenjson1= flattenjson.select(col("Students.user.*"),
							col("address.*"),explode(col("address.streets")),col("orgname"),col("trainer"))
					flattenjson1.printSchema()
					
					val flattenjson2=flattenjson1.withColumn("streets", explode(col("streets")))
					flattenjson2.printSchema()
					flattenjson2.show()			
					
					
	
	}
}