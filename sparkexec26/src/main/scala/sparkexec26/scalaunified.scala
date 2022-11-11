package sparkexec26
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object scalaunified {

	def main(args:Array[String]):Unit={


			val conf= new SparkConf().setAppName("first").setMaster("local[*]")
					val sc= new SparkContext(conf)

					sc.setLogLevel("ERROR")
					val spark=  SparkSession.builder().getOrCreate()
					import spark.implicits._

					val df=spark.read.format("csv").option("header","true").load("file:///c:/data/usdata.csv")
					df.createOrReplaceTempView("usdatadf")
					val fildf= spark.sql("select * from usdatadf where state='LA'")
					fildf.show()


					val df2=spark.read.format("csv").option("delimiter","~").option("header","true").load("file:///c:/data/txns_head")
					df2.createOrReplaceTempView("delimiteddf")
					val defildf= spark.sql("select * from delimiteddf where spendby='cash'")
					defildf.show()

	}

}