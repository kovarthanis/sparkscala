package greaterspark
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._

object rdd_24 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd_24")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()

    println("==============eg1===========")
    val data = sc.textFile("file:///c:/data/regiondata.txt")
    val datasplit = data.flatMap(x => x.split("~"))
    val statelist = datasplit.filter(x => x.contains("State")).map(x => x.replace("State->", ""))
    val citylist = datasplit.filter(x => x.contains("City")).map(x => x.replace("City->", ""))

    println("==============state list===========")
    statelist.foreach(println)
    println("==============city list===========")
    citylist.foreach(println)

    println("==============eg2===========")

    val data1 = sc.textFile("file:///c:/data/txns")
    val fildata1 = data1.filter(x => x.contains("Gymnastics"))
    fildata1.foreach(println)
    val fildata2 = data1.filter(x => x.contains("Team Sports"))
    fildata2.foreach(println)

    println("==============union output===========")

    val uniondata = fildata1.union(fildata2)
    uniondata.foreach(println)
  
        println("==============eg3===========")
        
        val data2=sc.textFile("file:///c:/data/usdata.csv")
        val fillength=data2.filter(x=> x.length()>200)
        .flatMap(x=> x.split(","))
        .map(x=> x+ ",zeyo")
        .map(x=>x.replace("-", "")).saveAsTextFile("file:///c:/data/eg3.txt")
            
        println("=================file written================")
            

  
  
  }
  
  
}