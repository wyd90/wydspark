package com.wyd.spark.core.favt

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavtOne {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("favtOne").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val subjectAndTeacher = sc.textFile("D:\\yarnData\\favt\\input\\teacher.log")
      .map(line => {
        val arr = line.split("/")
        val strings = arr(2).split("[.]")
        val subject = strings(0)
        val teacher = arr(3)
        ((subject, teacher), 1)
      })

    val reduced = subjectAndTeacher.reduceByKey(_ + _)
    val res: RDD[(String, (String, Int))] = reduced.map(x => {
      (x._1._1, (x._1._2, x._2))
    }).groupByKey().mapValues(it => {
      val tuples = it.toList.sortBy(v => v._2).reverse.take(1)
      tuples(0)
    })
    println(res.collect().toBuffer)

    sc.stop()
  }

}
