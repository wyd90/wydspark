package com.wyd.spark.streaming.orderstatic

import com.wyd.redis.JedisUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object OrderStatic {

  def contextFunc() = {
    val conf = new SparkConf().setAppName("order_static").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Duration(5000))
    ssc.checkpoint("file:///D:\\sparkck")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "order_consumer",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("orderMsg")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()){
//        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val lines = rdd.map(_.value())

        val filtered = OrderStaticUtil.etlData(lines)

        OrderStaticUtil.calculateGMV(filtered)

        OrderStaticUtil.calculateGMVByProvince(filtered, filtered.sparkContext)

//        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    }


    ssc
//    ssc.start()
//    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    val context = StreamingContext.getOrCreate("file:///D:\\sparkck", contextFunc _)
    context.start()
    context.awaitTermination()
  }

}
