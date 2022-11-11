package spark24
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object sparkobj {
  
  def main(args:Array[String]):Unit={
    
    val conf= new SparkConf().setAppName("first").setMaster("local[*]")
    
    val sc=new SparkContext(conf)
    
    sc.setLogLevel("Error")
    
    val data=sc.textFile("file:///C:/data/txns")
    
    val gymdata=data.filter(x => x.contains("Gymnastics"))
    
      
    val teamdata=data.filter(x => x.contains("Team Sports"))
    
    val uniondata=gymdata.union(teamdata)
    
     uniondata.foreach(println)
    
    
        
    
    
  }
  
  
}