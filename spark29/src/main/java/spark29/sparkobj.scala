package spark29

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


					println("==================task 1================")

					val readdevices= spark.read.format("json").load("file:///c:/data/devices.json")

					val filreaddevices=readdevices.filter(col("lat")>70)

					filreaddevices.write.format("parquet").mode("overwrite").save("file:///c:/data/devicesout")

					println("===============file written=============")

					println("==================task 2================")

					val readtxnshead= spark.read.format("csv").option("delimiter","~").option("header","true").load("file:///c:/data/txns_head")

					val readexpr = readtxnshead.selectExpr("txnno","split(txndate,'-')[0] as month","custno","amount","category","product","city","state","spendby")
					
					readexpr.show()
					
					
					println("==================task 3================")
					
					val filreadtxnshead = readtxnshead.filter(col("category")==="Gymnastics")
					
					val filhead = filreadtxnshead.selectExpr("txnno","txndate","custno","amount","category","product","city","state","spendby","case when spendby='credit' then 1 " + "else 0 end as check" )
					
					filhead.show()
	}



}