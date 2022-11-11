package sparkexec26

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object scalaobj {

	def main(args:Array[String]):Unit={

			println("==========ROW RDD=============")

			val conf=new SparkConf().setAppName("first").setMaster("local[*]")
			val sc= new SparkContext(conf)
			val spark= SparkSession.builder().getOrCreate()
			import spark.implicits._

			sc.setLogLevel("ERROR")

			val data=sc.textFile("file:///c:/data/txnsmall.txt")
			val mapsplit= data.map(x => x.split(","))
			val rowrdd=mapsplit.map(x => Row(x(0),x(1),x(2)))
			rowrdd.foreach(println)

			val filterrowrdd=rowrdd.filter(x => x(2).toString().contains("Gymnastics"))
			filterrowrdd.foreach(println)

			val schemastruct= new StructType()
			.add("txnno",StringType)
			.add("txndate",StringType)
			.add("category",StringType)

			val dffromrowrdd = spark.createDataFrame(filterrowrdd,schemastruct)
			dffromrowrdd.show()
	}

}