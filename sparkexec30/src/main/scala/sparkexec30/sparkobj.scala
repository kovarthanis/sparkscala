package sparkexec30

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object sparkobj {


	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

					val readtxnshead= spark.read.format("csv")
					.option("delimiter","~")
					.option("header","true")
					.load("file:///c:/data/txns_head")
					readtxnshead.show()

				//	val filreadtxnshead = readtxnshead.filter(col("category")==="Gymnastics")

				//	val filhead = filreadtxnshead
				//	.selectExpr("txnno","txndate","custno","amount","category","product","city","state","spendby","case when spendby='credit' then 1 " + "else 0 end as check" )

				//	val processeddf = filreadtxnshead.withColumn("check", expr("case when spendby='credit' then 1 " + "else 0 end"))
				//	processeddf.show()
          
					
				//	val processdf = filreadtxnshead.filter(col("category")==="Gymnastics").withColumn("txndate",expr("split(txndate,'-')[2]")).withColumnRenamed("txndate","year").withColumn("check", expr("case when spendby='credit' then 1 " + "else 0 end"))
					
					val processloc = readtxnshead.withColumn("city", expr("concat(city,'-',state)")).withColumnRenamed("city","Location").drop("city").drop("state")
					processloc.show()

	}
}