package com.yuan.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


object SparkCore {
	def main(args: Array[String]): Unit = {
			val conf = new SparkConf().setAppName("commission");
			val sc = new SparkContext(conf);
			val text = sc.textFile("/cpic/life/check/sort");
			val data:RDD[String] = text.filter {_.contains("female")}
			val femaleData:RDD[(String,Int)] = data.map{line =>
			val t= line.split(',')
			(t(0),t(2).toInt)
			}.reduceByKey(_ + _)
			val result = femaleData.filter(_._2>120)
			result.foreach(println)
	}
}
