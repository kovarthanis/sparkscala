package sparkexec29
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


					val readdata= spark.read.format("csv").option("delimiter","~").option("header","true").load("file:///c:/data/txns_head")
					val filreaddata= readdata.filter(  col("category") === "Gymnastics" 
					&& col("spendby")==="cash")
					val multifilreaddata = readdata.filter(col("category").isin ("Gymnastics","Team Sports") )
					multifilreaddata.show()
					val filnotequals= readdata.filter(  !(col("category") === "Gymnastics") )

					val filcontains=readdata.filter((col("category") === "Gymnastics" )
							&& (col("product") like ("%Gymnastics%")))


					val schemastructype = new StructType()
					.add("id",IntegerType)
					.add("category",StringType)
					.add("name",StringType)

					val readdata1= spark.read.format("csv").schema(schemastructype).option("delimiter",",").load("file:///c:/data/txnsmall.csv")
					readdata1.show()

					val filternul = readdata1.filter((col("name") isNull))

					filternul.show()
					val filternotnul = readdata1.filter(!(col("name") isNull))

					filternotnul.show()
	}
}