package sparkexec36
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


					val file1=spark.read.format("csv").option("header","true").load("file:///c:/data/file1.txt")
					val file2=spark.read.format("csv").option("header","true").load("file:///c:/data/file2.txt")
					//val joindf= file1.join(file2,Seq("id"),"inner") 
				//	val joindf= file1.join(file2,file1("id")===file2("custid"),"inner").drop(file2("custid"))

	}
}