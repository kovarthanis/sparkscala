package spark37

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util.Calendar
import java.text.SimpleDateFormat
import scala.io.Source

object sparkobj {

	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val spark = SparkSession.builder()
					.config("fs.s3a.access.key","AKIAQ54J4EOPSYAELO4X")
					.config("fs.s3a.secret.key","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")
					.getOrCreate()
					
					import spark.implicits._


					println("========================================TASK 1===================================")
					val df=spark.read.format("csv").load("s3a://ko-zeyo/file1.txt")
					df.show()

					println("========================================TASK 2===================================")
					val cal=Calendar.getInstance().getTime
					val dformat= new SimpleDateFormat("yyyy-MM-dd")
				
					val today= dformat.format(cal)	
					println(today)
					val df1=spark.read.format("avro").load(s"file:///c:/data/project/$today/*")
					df1.show()
	}	
}