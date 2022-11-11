package sparkexec25

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object scalaobj1 {
  
  case class schema(txnsno:String,category:String,product:String)
  
  def main(args:Array[String]):Unit ={
    val conf=new SparkConf().setAppName("first").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val data=sc.textFile("file:///c:/data/txnsmall.txt")
    
    val spark= SparkSession.builder().getOrCreate()
    
    import spark.implicits._
    
    val mapsplit= data.map(x=> x.split(","))
    val schemadata=mapsplit.map( x=> schema(x(0),x(1),x(2)))
    
    val df= schemadata.toDF()
    df.show()
    
    df.createOrReplaceTempView("zeyodf")
    val df1=spark.sql("select * from zeyodf where category='Gymnastics'")
    
    df1.show()
  
  }
  
}