package sparkexec34
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.io.Source._

object sparkobj1 {

	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val spark =  SparkSession.builder().getOrCreate()
					import spark.implicits._

					val urldata=fromURL("https://randomuser.me/api/0.8/?results=10").mkString

					println(urldata)

					val rdd=sc.parallelize(List(urldata))

					val df= spark.read.json(rdd)
					df.show()
					df.printSchema()

					val flattendf= df.withColumn("results",explode(col("results")))
					flattendf.printSchema()

					val moreflatterndf=flattendf.select(col("nationality"),
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
							col("results.user.username"),
							col("seed"),col("version"))
					moreflatterndf.printSchema()
					moreflatterndf.show()
	}

}