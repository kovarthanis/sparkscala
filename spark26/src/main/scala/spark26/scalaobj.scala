package spark26

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object scalaobj {

	def main(args:Array[String]):Unit={

			val conf= new SparkConf().setAppName("first").setMaster("local[*]")
					val sc =new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark=  SparkSession.builder().getOrCreate()
					import spark.implicits._
					
					println("=============JSON Datasource==========")

					val jsondf=spark.read.format("json").load("file:///C:/data/devices.json")
					jsondf.show()
					
					jsondf.createOrReplaceTempView("jsondfdevice")
					
					val devicedf = spark.sql("select * from jsondfdevice where humidity>70")
					//devicedf.write.format("txt").option("header","true").save("file:///C:/data/wdata/txtdir")
				//jsondf.write.format("xml").save("file:///C:/data/wdata/xmldir")
					jsondf.write.format("parquet").save("file:///C:/data/wdata/parquetdir1")
					jsondf.write.format("avro").save("file:///C:/data/wdata/avrodir")
					
						println("=============processed ouputs===========")
					
					println("=============ORC Datasource===========")

					val orcdf=spark.read.format("orc").load("file:///C:/data/orcdata.orc")
					orcdf.show()
					
					println("=============AVRO Datasource==========")
					
					val avrodf=spark.read.format("avro").load("file:///C:/data/part_av.avro")
					avrodf.show()
					
					avrodf.createOrReplaceTempView("agedata")
					
					val agedatashow = spark.sql("select * from agedata where age > 30")

					println("=============age greater than 30=============")
					
					agedatashow.show() 
					
					agedatashow.write.format("parquet").mode("error").save("file:///C:/data/parquetop/part_av.parquet")
					agedatashow.write.format("parquet").mode("append").save("file:///C:/data/parquetop/part_av.parquet")
				agedatashow.write.format("parquet").mode("ignore").save("file:///C:/data/parquetop/part_av.parquet")
					agedatashow.write.format("parquet").mode("overwrite").save("file:///C:/data/parquetop/part_av.parquet")
					agedatashow.write.format("parquet").mode("error").save("file:///C:/data/parquetop/part_av.parquet3") 
	}

}