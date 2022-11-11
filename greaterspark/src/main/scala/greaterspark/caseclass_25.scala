package greaterspark
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j._


object caseclass_25 {
  
  case class schema(txnno:String, category: String, product: String )
  
    case class txnschema(txnno:String, txndate: String,custno: String, amount: String, category: String, product: String, city : String, state: String, spendby: String )

  
  
 // val txnschema=new StructType().add("txnno",StringType, nullable=true).add("category",StringType, nullable=true).add("product",StringType, nullable=true)
  
  def main(args: Array[String]):Unit={
    
    val conf=new SparkConf().setAppName("caseclass").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._
    
    val data=sc.textFile("file:///c:/data/txnsmall.txt")
    val splitdata=data.map(x=> x.split(","))
    val schdata=splitdata.map(x=> schema(x(0),x(1),x(2)))
    val fildata=schdata.filter(x=> (x.product.contains("Gymnastics")))
    fildata.foreach(println)
    
   val df1= fildata.toDF()
   df1.show()
  
  println("====================task1================")
  val data1=sc.textFile("file:///c:/data/txns")
  val splitdata1=data1.map(x=> x.split(","))
  val txnsc= splitdata1.map(x=> txnschema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
  val finres=txnsc.filter(x=> x.spendby.contains("cash"))
  finres.foreach(println)
  
  
    println("====================task2================")
    
    val df2=spark.createDataFrame(txnsc)
    df2.createOrReplaceTempView("zoom")
    val op=spark.sql("select * from zoom where spendby='credit'")
    op.show()
//finres1.show()
  }
  
}