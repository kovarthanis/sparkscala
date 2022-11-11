package sparkpack

import org.apache.spark._
import sys.process._

object sparkobj {

def main(args:Array[String]):Unit=
{

val conf = new SparkConf().setAppName("first").setMaster("local[*]")
val sc = new SparkContext(conf)
sc.setLogLevel("ERROR")

val data = sc.textFile("file:///home/cloudera/data/txns")
val gymdata = data.filter(x=>x.contains("Gymnastics"))
"hadoop fs -rmr /user/cloudera/datagym ".!
gymdata.saveAsTextFile("/user/cloudera/datagym")


}

}