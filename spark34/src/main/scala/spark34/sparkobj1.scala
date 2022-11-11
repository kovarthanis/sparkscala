package spark34

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ 


object sparkobj1 {

	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val spark =  SparkSession.builder().getOrCreate()
					import spark.implicits._
					val colsschema=  StructType(Array(StructField("id",StringType),StructField("name",StringType)))
					val readstreams = spark.readStream.format("csv").schema("colsschema").load("file:///C:/data/streams/inpstream/data1")
					readstreams.writeStream.format("parquet").option("checkpointLocation", "file:///C:/data/streams/opstream/").start().awaitTermination()

	}
}