package greaterspark
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._

object flatmaprdd {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("first")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()

    val liststr = List("azeyobron~analytics", "zeyo~bigdata", "hive~zeyo")
    println("=========flat and split=========")

    val processed = liststr
      .flatMap(x => x.split("~"))
      .filter(x => x.contains("zeyo"))
      .map(x => x.replace("zeyo", "ko"))
      .map(x => x + ",var")

    processed.foreach(println)

    println("========with remote data=====")
    val data = sc.textFile("file:///c:/data/zeyodata.txt")
    val finalprocess = data.flatMap(x => x.split("~"))
      .filter(x => x.contains("zeyo"))
      .map(x => x.replace("zeyo", "ko"))
      .map(x => x + ",var")

    finalprocess.foreach(println)
    
    
    
    println("===================task============")
    val liststr2=List("State->telangana~City->hyd")
    val datasplit=liststr2.flatMap(x=> x.split("~"))
    val statelist=datasplit.filter(x=> x.contains("State")).map(x=> x.replace("State->",""))
    val citylist=datasplit.filter(x=> x.contains("City")).map(x=> x.replace("City->",""))
    statelist.foreach(println)
    citylist.foreach(println)
    
  }
}