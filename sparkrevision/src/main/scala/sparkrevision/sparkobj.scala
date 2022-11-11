package sparkrevision

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

object sparkobj {


case class columnschema ( device_id:String,device_name:String,humidity:String,lat:String,longt:String, scale:String,temp:String,timestamp:String,zipcode:String)

def main(args:Array[String]):Unit={

		val conf = new SparkConf().setAppName("first").setMaster("local[*]")
				val sc = new SparkContext(conf)
				sc.setLogLevel("ERROR")
				val spark =   SparkSession.builder().getOrCreate()
				import spark.implicits._
				val collist = List("device_id","device_name","humidity","lat","longt","scale","temp","timestamp","zipcode")

				println("===============1. create a scaala list with 1,4,6,7 and do an iteration and add 2 in it==============")

				val firstlist = List(1,4,6,7)
				val procfirstlist = firstlist.map( x => x+2)
				procfirstlist.foreach(println)


				println("================2. create a scala list with Zeyobron,zeyo and analytics and filter elements contains zeyo==============================")
				val secondlist= List("Zeyobron","zeyo","analytics")

				val procsecondlist = secondlist.filter(x => x.contains("zeyo"))
				procsecondlist.foreach(println)

				println("================3.Read file1 as an rdd and filter gymnastics rows ==============================")


				val readfile1= sc.textFile("file:///c:/data/rdata/file1.txt")
				val readfilefil = readfile1.filter( x => x.contains("mac"))
				readfilefil.foreach(println)


				println("================4.Create a case class and impose case class to it for schema rddand filter product contains gymanstics ==============================")

				val splitreadfile1= readfile1.map(x => x.split(","))
				val columnschema1= splitreadfile1.map(x => columnschema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
				val schemadf= columnschema1.filter(x => x.device_name.contains("mac")).toDF().select(collist.map(col):_*)

				schemadf.show()

				println("================5. read file2 and convert it to row rdd==============================")

				val readfile2 = sc.textFile("file:///c:/data/rdata/file2.txt")
				val splitreadfile2 = readfile2.map(x => x.split(","))
				val readfile2rdd= splitreadfile2.map(x => Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))

				readfile2rdd.foreach(println)



				println("================6. create dataframe using schema rdd and row rdd==============================")

				val structtypeschema = new StructType()
				.add("device_id",StringType)
				.add("device_name",StringType)
				.add("humidity",StringType)
				.add("lat",StringType)
				.add("longt",StringType)
				.add("scale",StringType)
				.add("temp",StringType)
				.add("timestamp",StringType)
				.add("zipcode",StringType)


				val rowdf=spark.createDataFrame(readfile2rdd,structtypeschema).select(collist.map(col):_*)
				rowdf.show()
				//val filesdf1=	filesdf


				println("================7.  read file3 as csv awith header true ==============================")

				val csvdf=spark.read.format("csv").option("delimiter",",").option("header","true").load("file:///c:/data/rdata/file3.csv").select(collist.map(col):_*)

				csvdf.show()

				println("================8. read file 4 aas json and file 5 as parquet and show both dataframe==============================")
				val jsondf= spark.read.format("json").option("header","true").load("file:///c:/data/rdata/file4.json").select(collist.map(col):_*)

				val pardf= spark.read.option("header","true").load("file:///c:/data/rdata/file5.parquet").select(collist.map(col):_*)

				jsondf.show()
				pardf.show()



				println("================9. Read file6 as xml with tag txndata and show the datframe==============================")

				val xmldf = spark.read .format("com.databricks.spark.xml") .option("rowTag","Transaction") .load("file:///c:/data/rdata/file6s.xml")
				xmldf.show()
				xmldf.printSchema()

			/*	println("================10.  read data from aws rds fro cash datta==============================")

				val readawsrds = spark.read.format("jdbc")
				.option("driver","com.mysql.jdbc.Driver")
				.option("url","jdbc:mysql://database-1.cvk5bls6gwai.ap-south-1.rds.amazonaws.com:3306/zeyodb")
				.option("dbtable","zeyotab")
				.option("username","root")
				.option("password","Aditya908")
				.option("useSSL","false").load()
				readawsrds.show() */

				println("================11. find the cumulative sum of amount for each categorye==============================")
				val readtxnshead= spark.read.format("csv")
				.option("delimiter","~")
				.option("header","true")
				.load("file:///c:/data/txns_head")
				val sumofall= readtxnshead.groupBy(col("category")).agg(sum(col("amount")).as("total"))
				sumofall.show()


				println("================12. write as an avro in local with mode append and partition the category column==============================")

				sumofall.write.format("avro").mode("overwrite").save("file:///c:/data/rdata/avrodata")
				println("=========avro file written====================")

				println("================13. Define a unified column list and impose using select for all hte datframe and union all the dataframese==============================")
				val uniondf= schemadf.union(rowdf).union(csvdf).union(pardf).union(jsondf)		
				uniondf.show()
				//df.select(columns.map(col):_*)
				//	val df = filesdfselect(collist.map(col):_*)

				println("================14. from union df get year from txn date and rename it with year and add one column at end as status 1 for cash and 0 for credit in spendby and filter txnno> 50000")

				 val procdf= uniondf.withColumn("device_name",expr("split(device_name,'-')[1]"))
				.withColumnRenamed("device_name","deviceused")
								.filter(col("humidity")>90) 
								procdf.show()
} 

}