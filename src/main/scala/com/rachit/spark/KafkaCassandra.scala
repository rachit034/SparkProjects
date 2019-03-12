package com.rachit.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds,StreamingContext}
import org.apache.spark.storage.StorageLevel

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._

import com.datastax.spark.connector._

import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder

object KafkaCassandra {
  /**
 * Listens to a stream of weblogs on Kafka producer using sparkstreaming and store the data to cassandra database
 *  over a 10 minute batch interval.
 */
  
  def main(args: Array[String]){
    
    val conf = new SparkConf().set("spark.cassandra.connection.host", "127.0.0.1").setMaster("local[*]").setAppName("KafkaCassandra")
    
    val ssc = new StreamingContext(conf,Seconds(10))
    
    setupLogging()
    
    val pattern = apacheLogPattern()
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val topics = List("testLogs").toSet
    
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics).map(_._2)
         
    
    val requests = lines.map(x => {
    val matcher:Matcher = pattern.matcher(x)
    if (matcher.matches()) {
      val ip = matcher.group(1)
      val request = matcher.group(5)
      val requestFields = request.toString().split(" ")
      val url = scala.util.Try(requestFields(1)) getOrElse "[error]"
      (ip, url, matcher.group(6).toInt, matcher.group(9))
    } else {
      ("error", "error", 0, "error")
    }
  })
    
    requests.foreachRDD((rdd, time) => {
      rdd.cache()
      println("Writing " + rdd.count() + " rows to Cassandra")
      rdd.saveToCassandra("frank", "LogTest", SomeColumns("IP", "URL", "Status", "UserAgent"))
    })
    
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination() 
    
    
  }
}