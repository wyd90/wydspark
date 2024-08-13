package com.wyd.spark.core.favt

import org.apache.spark.{SparkConf, SparkContext}

object FavtTwo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("favtTwo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val reduced = sc.textFile("D:\\yarnData\\favt\\input")
      .map(line => {
        val arr = line.split("[/]")
        val subject = (arr(2).split("[.]")) (0)
        val teacher = arr(3)
        ((subject, teacher), 1)
      }).reduceByKey(_ + _)

    reduced.cache()

    val subjects = reduced.map(x => x._1._1).distinct().collect()

    for (subject <- subjects) {
      val filtered = reduced.filter(x => {
        val v = x._1._1
        if (v == subject) {
          true
        } else {
          false
        }
      })

      val res = filtered.sortBy(_._2, false).take(1)
      println(res(0))
    }

    reduced.unpersist(true)
    sc.stop()
  }

}
