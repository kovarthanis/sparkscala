package greaterspark
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object cdp35 {
  
  
  def main(args:Array[String]):Unit={
    
  
 val conf=new SparkConf().setMaster("local[*]").setAppName("cdpl")
 val sc=new SparkContext(conf)
  sc.setLogLevel("ERROR")
  val spark=SparkSession.builder().getOrCreate()

  val data=spark.read.format("json").option("multiline",true).load("file:///c:/data/complexdata/users.json")
  data.printSchema()
  val flatdata=data.withColumn("users",explode(col("users.user")))
  val moreflat=flatdata.select("page","per_page","total","total_pages","users.*")
  moreflat.printSchema()
  
  val deflat=moreflat.select(struct("emailAddress","firstName","lastName","phoneNumber","userId").alias("user"))
val deflat1=deflat.groupBy("page","per_page","total","total_pages").agg(collect_list("user"))
deflat1.printSchema()
  }
  
}