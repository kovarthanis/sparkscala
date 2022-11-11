package sparkexec33
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
					println("==============handson 1==================")

					val zeyojson= spark.read.format("json").option("multiline","true").load("file:///c:/data/complexdata/zeyo33.json")
					zeyojson.printSchema()
					zeyojson.show()

					val flattenzeyojson=
					zeyojson.select(
							col("address.permanentAddress"),
							col("address.temporaryAddress"),
							col("orgname"),
							col("trainer"),
							col("age")

							)
					flattenzeyojson.show()
					val revertflattenzeyojson = flattenzeyojson.
					select (struct(
							col("permanentAddress"),
							col("temporaryAddress")
							).as("address"),
							col("age"),
							col("orgname"),
							col("trainer"))

					revertflattenzeyojson.show()

					println("==============handson 2==================")
					val zeyocjson= spark.read.format("json").option("multiline","true").load("file:///c:/data/complexdata/zeyoc33.json")
					zeyocjson.printSchema()
					zeyocjson.show()

					val flattenzeyocjson=
					zeyocjson.select(
							col("address.permanentAddress.area").as("parea"),
							col("address.permanentAddress.location").as("ploc"),
							col("address.temporaryAddress.area").as("tarea"),
							col("address.temporaryAddress.location").as("tloc"),
							col("orgname"),
							col("trainer")

							)
					flattenzeyocjson.show()
					val revertflattenzeyocjson = flattenzeyocjson.
					select (struct(
							struct(col("parea"),col("ploc")).as("permanentAddress"),
							struct(col("tarea"),col("tloc")).as("temporaryAddress")
							).as("address"),
							col("orgname"),
							col("trainer"))
					revertflattenzeyocjson.show()

										println("==============complex data 3==================")

										val zeyocdf=spark.read.format("json").option("multiline","true").load("file:///c:/data/complexdata/zeyoc.json")
zeyocdf.printSchema()
zeyocdf.show()
val flattenzeyocdf = zeyocdf.select(explode(col("Students")).as("Students"),
    col("address.permanentAddress"),
    col("address.temporaryAddress"),
    col("orgname"),
    col("trainer"))
    flattenzeyocdf.show()
	}
}