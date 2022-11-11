package greaterspark
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row._
import org.apache.log4j._

object rowrdd {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("casesclass").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    val data = sc.textFile("file:///c:/data/txnsmall.txt")

    val sch = data.map(x => x.split(","))
    val rowrdd = sch.map(x => Row(x(0), x(1), x(2)))
    val filterrowrdd = rowrdd.filter(x => x(2).toString().contains("Gymnastics"))

    filterrowrdd.foreach(println)

    val schema = new StructType()
      .add("txnno", StringType)
      .add("category", StringType)
      .add("product", StringType)

    val df = spark.createDataFrame(filterrowrdd, schema)
    df.show()

    println("=================task 1============")

    val devicesdf = spark.read.format("json").load("file:///c:/data/devices.json")
    devicesdf.show()
    val orcdf = spark.read.format("orc").load("file:///c:/data/orcdata.orc")

    orcdf.show()
    val avrodf = spark.read.format("com.databricks.spark.avro").load("file:///c:/data/part_av.avro")
    avrodf.show()

    spark.stop()
  }
}