package greaterspark
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object cdp33 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("cdpnew")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val data = spark.read.format("json").option("multiline", true).load("file:///c:/data/complexdata/Pets.json")
    data.printSchema()
    val procdata1 = data.withColumn("pets", explode(col("Pets")))
    val procdata2 = procdata1.select(col("Address.*"), col("Mobile"), col("Name"), col("status"), col("Pets"))
    procdata2.show()
    
    
       val data1 = spark.read.format("json").option("multiline", true).load("file:///c:/data/complexdata/donut.json")
    data1.printSchema()
    val data2=data1.select(col("id"),col("image.height").alias("iheight"),col("image.url").alias("iurl"),col("image.width").alias("iwidfth"),col("name"),col("thumbnail.*"),col("type"))
 data2.show()
 val findata=data2.select(col("id"),struct(col("iheight"),col("iurl"),col("iwidth")).alias("image"),col("name"),struct(col("height"),col("url"),col("width")).alias("thumbnail"),col("type"))
 findata.printSchema()
  }

}