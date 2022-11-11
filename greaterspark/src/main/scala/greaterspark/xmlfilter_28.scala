package greaterspark

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._

object xmlfilter_28 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("xmlfil")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    println("========================task1===========")
    // val xmldf=spark.read.format("com.databricks.spark.xml").option("rowTag","POSLog").load("file:///c:/data/transactions.xml")
    // xmldf.show()

    println("================tas2==============")
    val readcsv = spark.read.format("csv").load("file:///c:/data/usdata.csv")
    val readcsvdf = readcsv.select("first_name", "last_name").filter("state=LA")
    readcsvdf.show()
  }

}