package sparkexec51


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

			val structureSchema = new StructType()
    .add("id",StringType)
      .add("name",StringType)


							val readstreams = spark.readStream.format("csv").schema(structureSchema).load("file:///C:/data/streams/inpstream")
							
							readstreams.writeStream.format("console").option("checkpointLocation", "file:///C:/data/streams/opstream").start().awaitTermination()

	}
}