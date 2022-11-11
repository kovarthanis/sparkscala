package sparkexec28
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession


object sparkobj {
  
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    
    val readmysql = spark.read.format("jdbc")
    .option("driver","com.mysql.jdbc.Driver")
    .option("url","jdbc:mysql://database-1.cvk5bls6gwai.ap-south-1.rds.amazonaws.com:3306/zeyodb")
    .option("dbtable","zeyotab")
    .option("user","root")
     .option("useSSL","false")
    .option("password","Aditya908")
    .load()
    
    //readmysql.createOrReplaceTempView("zeyo")
   // val readmysqldf= spark.sql("select * from zeyo where amount >40")
    
    //dsl
    
   val readmysqldf= readmysql.filter("amount>40")
    
    
    readmysqldf.show()
    
    readmysqldf.write.format("json").mode("overwrite").save("c:/data/mysqldf")
    
  //  readmysql.show()
    
    val readxml =spark.read.format("com.databricks.spark.xml")
    .option("rowTag","book")
    .load("file:///c:/data/book.xml")
    
    readxml.show()
    
    val readtransactxml = spark
    .read
    .format("com.databricks.spark.xml")
    .option("rowTag","POSLog")
    .load("file:///c:/data/transactions.xml")
    
    readtransactxml.show()
    readtransactxml.printSchema()
    
    //handson
    
    
    val csvfileread= spark.read.format("csv").option("header","true").load("file:///C:/data/usdata.csv")
    val csvfilereaddf=csvfileread.filter("state='LA'")
    csvfilereaddf.show()
  }
}