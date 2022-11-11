package sparkexec35
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
						
				/*	println("===========================handson1===================================")
					val readjson=spark.read.format("json").option("multiline","true").load("file:///c:/data/complexdata/zeyoc35.json")
					readjson.printSchema()
					readjson.show()
					val flattenjson= readjson.withColumn("Students",explode(col("Students")))
					flattenjson.printSchema()
					val moreflatten=flattenjson.select(col("Students.*"),col("orgname"),col("trainer"))
					moreflatten.printSchema()
				/*	val shrinkflatten = moreflatten.withColumn("Students",struct(col("age"),col("name"))).drop(col("age")).drop(col("name"))
					shrinkflatten.printSchema()
				val moreshrinkflatten=shrinkflatten.groupBy(col("orgname"),col("trainer")).agg(collect_list(col("Students")).as("Students"))
				moreshrinkflatten.printSchema()
					 */
				val altshrink=moreflatten.groupBy(col("orgname"),col("trainer")).agg(collect_list(
				   struct( col("age"),col("name")


				)).alias("Students"))
				altshrink.show()
					 
					println("===========================handson2===================================")
					val readjson1=spark.read.format("json").option("multiline","true").load("file:///c:/data/complexdata/users.json")
					readjson1.printSchema()
					val flattenjson1=readjson1.withColumn("users",explode(col("users"))).select(col("page"),col("per_page"),col("total"),col("total_pages"),col("users.user.*"))
					flattenjson1.printSchema()
					flattenjson1.show()
					val shrinkjson=flattenjson1
					.groupBy(col("page"),col("per_page"),col("total"),col("total_pages"))
					.agg(
							collect_list(
									struct(
											col("emailAddress"),
											col("firstName"),
											col("lastName"),
											col("phoneNumber"),
											col("userId"))).as("users"))
					shrinkjson.printSchema()
					shrinkjson.show() */
					
										println("===========================handson3 array inside array===================================")
val readjson2=spark.read.format("json").option("multiline","true").load("file:///c:/data/complexdata/zeyoc35c.json")
					readjson2.printSchema()
					val flattenreadjson2= readjson2.withColumn("Students", explode(col("Students")))
					.withColumn("tools",explode(col("Students.tools")))
					.select(col("Students.age"),col("Students.name"),col("orgname"),col("trainer"),col("tools"))
					
					
		//		val finalflattenreadjson2=flattenreadjson2.drop(col("Students.tools"))
				flattenreadjson2.printSchema()
				flattenreadjson2.show()
	}
}