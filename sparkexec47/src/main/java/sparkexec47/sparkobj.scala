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



object sparkobj { 

	def main(args:Array[String]):Unit={


			val conf= new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.allowMultipleContexts","true")
					val sc= new SparkContext(conf)
					val spark= SparkSession.builder().getOrCreate()
					import spark.implicits._
					sc.setLogLevel("ERROR")

					val kafkaParams = Map[String, Object](
							"bootstrap.servers" -> "localhost:9092",
							"key.deserializer" -> classOf[StringDeserializer],
							"value.deserializer" -> classOf[StringDeserializer],
							"group.id" -> "zeyo",
							"auto.offset.reset" -> "earliest",
							"enable.auto.commit" -> (false: java.lang.Boolean)
							)

					//val topics = Array("31tk","31tk2")
							val topics = Array("31tk3")
					val ssc= new StreamingContext(conf,Seconds(2))

					//group of rdd
					val stream = KafkaUtils.createDirectStream[String, String](
							ssc,
							PreferConsistent,
							Subscribe[String, String](topics, kafkaParams)
							).map(record => (record.value()))



					ssc.start()

					ssc.awaitTermination()
	}

}