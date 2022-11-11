package spark27
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._



object sparkpbj {

  
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc =new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

			
					 println("=======================TASK1======================")


					val loadcsvus= spark.read.format("csv").option("header","true").load("file:///c:/data/usdata.csv")
					loadcsvus.write.format("parquet").mode("overwrite").save("file:///c:/data/task1_27op/parusdata/")

					val loadparus = spark.read.format("parquet").load("file:///c:/data/task1_27op/parusdata/*.parquet")
					loadparus.write.format("json").mode("overwrite").save("file:///c:/data/task1_27op/jsonusdata")

					val loadjsonus= spark.read.format("json").load("file:///c:/data/task1_27op/jsonusdata/*.json")
					loadjsonus.write.format("orc").mode("overwrite").save("file:///c:/data/task1_27op/orcusdata")

					println("=======================Files written to destination======================")
				
					 
				  


						println("=======================TASK2======================")


					val df = spark.read.format("json").option("multiline","true").load("file:///C:/data/random10.json")
					
					

					df.printSchema()

}
}