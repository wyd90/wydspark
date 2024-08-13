package com.wyd.spark.core.favt

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object FavtThree {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("favtThree").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val maped = sc.textFile("D:\\yarnData\\favt\\input")
      .map(line => {
        val arr = line.split("[/]")
        val subject = (arr(2).split("[.]")) (0)
        val teacher = arr(3)

        ((subject, teacher), 1)
      })

    val subjects: Array[String] = maped.map(_._1._1).distinct().collect()
    val reduced = maped.reduceByKey(new FavtPartitioner(subjects), _ + _)

    import FavtImplicitUtil.favtOrder
    val res = reduced.mapPartitions(it => {
      val sortTree = new mutable.TreeSet[((String, String), Int)]()
      it.foreach(x => {
        sortTree.add(x)
        if (sortTree.size == 2) {
          val last = sortTree.last
          sortTree.remove(last)
        }
      })
      sortTree.toIterator
    }).collect()
    println(res.toBuffer)
  }

}

class FavtPartitioner(val subjects: Array[String]) extends Partitioner {

  val subjectMap = new mutable.HashMap[String, Int]()
  var i = 0
  for (subject <- subjects) {
    subjectMap.put(subject, i)
    i += 1
  }


  override def numPartitions: Int = i

  override def getPartition(key: Any): Int = {
    val tuple = key.asInstanceOf[(String, String)]
    subjectMap.getOrElse(tuple._1, 0)
  }

  override def hashCode(): Int = numPartitions;

  override def equals(obj: Any): Boolean = {
    obj match {
      case h: FavtPartitioner =>
        h.numPartitions == numPartitions
      case _ =>
        false
    }
  }
}
