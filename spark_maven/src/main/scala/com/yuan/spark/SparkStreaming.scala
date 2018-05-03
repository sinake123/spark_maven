package com.yuan.spark

import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder

object SparkStreaming {

	def main(args: Array[String]): Unit = {
			val batchDuration = Seconds(args(0).toInt)
					val windowDuration = Seconds(args(1).toInt)
					val topics = args(2)
					val brokers = args(3)
					val conf = new SparkConf().setAppName("collectionStreaming")
					val ssc = new StreamingContext(conf,batchDuration);
			ssc.checkpoint("checkpoint")
			val topicset = topics.split(",").toSet
			val kafkaParams = Map[String,String]("metadata.broker.list" -> brokers)
			val lines = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topicset)
			.map(_._2)
			lines.filter { _.contains("female") }.map { line => val t = line.split(",");(t(0),t(2).toInt) }
			.reduceByKeyAndWindow(_+_, _-_, windowDuration).filter(_._2>120).print()
			// StreamingœµÕ≥∆Ù∂Ø
			ssc.start()
			ssc.awaitTermination()
	}
}