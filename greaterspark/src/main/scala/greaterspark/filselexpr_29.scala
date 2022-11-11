package greaterspark
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._


object filselexpr_29 {
  
  def main(args:Array[String]):Unit={
    
    val conf=new SparkConf().setAppName("filexpr").setMaster("local[*]")
    val sc= new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark=SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val data=spark.read.format("json").load("file:///c:/data/Devices.json")
  //  data.printSchema()
    val fildata=data.filter(col("lat")>70)
    
   fildata.show()
   
   fildata.write.mode("ignore").save("file:///c:/data/fildata")
   

   
   println("=====================task2===============")
   
   val data1=spark.read.format("csv").option("delimiter","~").option("header","true").load("c:/data/txns_head")
  data1.printSchema()
   val fildata1= data1.selectExpr("txnno","custno","category","split((txndate),'-')[1] as month")
  
   
   println("===================task3=========")
   
   val data2=spark.read.format("csv").load("c:/data/txns_head")
  val procdata2=data2.filter("category=Gymnastics")
  procdata2.show()
  }
  
}