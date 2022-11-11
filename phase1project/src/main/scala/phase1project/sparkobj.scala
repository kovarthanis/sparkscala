package phase1project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io.Source._



object sparkobj {

	def main(args:Array[String]):Unit={

			val conf=new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("Error")
					val spark =  SparkSession.builder().getOrCreate()
					import spark.implicits._

					println("===========Step 2==============")

					val readavro=spark.read.format("avro").load("file:///c:/data/projectsample.avro")
					readavro.show()


					println("===========Step 3==============")

					val readurl=fromURL("https://randomuser.me/api/0.8/?results=1000").mkString
					val rdd = sc.parallelize(List(readurl))
					val urldf=spark.read.json(rdd)
					urldf.show()
					urldf.printSchema()

					println("===========Step 4==============")

					val flattenurldf= urldf
					.withColumn("results",expr("explode(results)"))
					.select (col("nationality"),
							col("results.user.cell"),
							col("results.user.dob"),
							col("results.user.email"),
							col("results.user.gender"),
							col("results.user.location.*"),
							col("results.user.md5"),
							col("results.user.name.*"),
							col("results.user.password"),
							col("results.user.phone"),
							col("results.user.picture.*"),
							col("results.user.registered"),
							col("results.user.salt"),
							col("results.user.sha1"),
							col("results.user.sha256"),
							col("results.user.username")
							)

					flattenurldf.printSchema()
					flattenurldf.show()

					println("===========Step 5==============")

					val remnumdf = flattenurldf.withColumn("username",regexp_replace($"username","[0-9]+",""))
					remnumdf.show()

					println("===========Step 6==============")

					println(readavro.count())
					println(remnumdf.count())
					val broadcastjoin=  readavro.join(broadcast(remnumdf),Seq("username"),"left")
					broadcastjoin.show()

					println("===========Step 7==============")

					println("===========Step 7a==============")
					val unavailablecustomers= broadcastjoin.filter(col("nationality") isNull)
					unavailablecustomers.show()
					println("===========Step 7b==============")
					val availablecustomers= broadcastjoin.filter(!(col("nationality") isNull))
					availablecustomers.show()


					println("===========Step 8==============")

					val nullhandle=unavailablecustomers.na.fill("Not Available").na.fill(0).show()


					println("===========Step 9==============")

					val finalunavailablecustomers=unavailablecustomers.withColumn("current_date",expr("current_date()"))
					val finalavailablecustomers=availablecustomers.withColumn("current_date",expr("current_date()"))
					finalunavailablecustomers.show()
					finalavailablecustomers.show()
	}
}