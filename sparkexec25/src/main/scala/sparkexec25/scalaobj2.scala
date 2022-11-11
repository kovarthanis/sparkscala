package sparkexec25

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object scalaobj2 {
  
  case class schema(txnno:String,txndate:String,custno:String,amount:String,category:String,product:String,city:String,state:String,spendby:String)

  def main(args:Array[String]):Unit =
  {
    
    val conf=new SparkConf().setAppName("first").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val data=sc.textFile("file:///c:/data/txns")
    val splitdata=data.map(x=> x.split(","))
    val schemadata=splitdata.map(x=> schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
    val spark = SparkSession
  .builder()
  .getOrCreate()
    
   import spark.implicits._
   
   val df=schemadata.toDF()
  // df.show()
   
   df.createOrReplaceTempView("cashdata")
   val df1=spark.sql("select * from cashdata where spendby='cash'")
   df1.show()
   
   
    
    
    
  }
  
}