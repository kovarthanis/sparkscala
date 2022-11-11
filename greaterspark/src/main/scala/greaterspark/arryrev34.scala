package greaterspark
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import scala.io.Source._

object arryrev34 {
  def main(args:Array[String]):Unit={
    
    val conf= new SparkConf().setMaster("local[*]").setAppName("arrayrev")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark=SparkSession.builder().getOrCreate()
    
    val readjsonarray= spark.read.format("json").option("multiline","true").load("file:///c:/data/complexdata/zeyoc.json")
					readjsonarray.show()
					readjsonarray.printSchema()
					
					val procreadjsonarray=readjsonarray.withColumn("Students", explode(col("Students")))
val moreproc=procreadjsonarray.select(col("Students"), col("address.permanentAddress"),col("address.temporaryAddress"),col("orgname"),col("trainer"))
  moreproc.printSchema()
  
  val revarr=moreproc.groupBy(struct(col("permanentAddress"),col("temporaryAddress")).alias("address"),col("orgname"),col("trainer")).agg(collect_list("Students"))
revarr.printSchema()
revarr.show()


println("==================from url==========")
val  datajson=fromURL("https://randomuser.me/api/0.8?results=10").mkString // read from url and convert t as string
 
val rdd=sc.parallelize(List(datajson)) //convert string to rdd

val df=spark.read.json(rdd) //convert rdd to datframme

df.show()
  }
}