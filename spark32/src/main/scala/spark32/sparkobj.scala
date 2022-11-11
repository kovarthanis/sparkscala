package spark32

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
					println("======task 1=============")
					val zeyodf= spark.read.format("json").option("multiline","true").load("file:///c:/data/complexdata/zeyoc.json")


					zeyodf.printSchema()


					val flattenzeyodf=	zeyodf.select(col("address.*")
							,col("orgname")
							,col("trainer"))
					flattenzeyodf.show()

					println("======task 2=============")
					val placedf= spark.read.format("json").option("multiline","true").load("file:///c:/data/complexdata/place.json")
					placedf.printSchema()


					val flattenplacedf =placedf.select(col("place"),
							col("user.address.*"),
							col("user.name"))
					flattenplacedf.show()

					println("======task 3=============")
					val toppingdf= spark.read.format("json").option("multiline","true").load("file:///c:/data/complexdata/topping.json")
					toppingdf.printSchema()

					val flattentoppingdf = toppingdf.select(col("batters.batter.id").as("bid"),
							col("batters.batter.type").as("btype"),
							col("id"),
							col("name"),
							col("ppu"),
							col("topping.*"),
							col("type"))
					flattentoppingdf.show()
					//val flattenzeyodf= zeyodf.select("")

	}
}