package greaterspark
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Row._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object writessavemodes_27 {
  
  def main(args: Array[String]):Unit={
    
    val conf=new SparkConf().setAppName("writesavemodes").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark= SparkSession.builder().getOrCreate()
    
    import spark.implicits._
    
    
    val avrodf=spark.read.format("com.databricks.spark.avro").load("file:///c:/data/part_av.avro")
    
    val adf=avrodf.createOrReplaceTempView("avrodata")
    
    
    val querydf= spark.sql("select * from avrodata where age>30")
    
    querydf.show()
    
    querydf.write.format("parquet").mode("overwrite").save("file:///c:/data/avro_p")
    
    
    println("=======================task1===========")
    
    val randdf=spark.read.format("json").load("file:///c:/data/randomuser.json")
    randdf.show()
    
    randdf.printSchema()
    
  }
  
}