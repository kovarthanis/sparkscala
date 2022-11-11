package sparkexec47

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import scala.io.Source
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream

object sparkobj { 

  def b2s(a: Array[Byte]): String = new String(a)
	def main(args:Array[String]):Unit={


			val conf= new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.allowMultipleContexts","true")
					val sc= new SparkContext(conf)
					val spark= SparkSession.builder().getOrCreate()
					import spark.implicits._
					sc.setLogLevel("ERROR")



					//val topics = Array("31tk","31tk2")
					//val topics = Array("31tk3")
					val ssc= new StreamingContext(conf,Seconds(2))

					val kinesisStream = KinesisInputDStream.builder
					.streamingContext(ssc)
					.endpointUrl("https://kinesis.ap-south-1.amazonaws.com")
					.regionName("ap-south-1")
					.streamName("kinesismq")
					.initialPositionInStream(InitialPositionInStream.TRIM_HORIZON )
					.checkpointAppName("kine")
					.checkpointInterval(Duration(2000))
					.storageLevel(StorageLevel.MEMORY_AND_DISK_2)

//val fstream=kinesisStream.map(x => b2s(x))


		val fst=	kinesisStream.build().map(x => b2s(x))
fst.print()

			ssc.start()

			ssc.awaitTermination()
	}

}