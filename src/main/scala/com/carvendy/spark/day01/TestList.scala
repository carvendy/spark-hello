package com.carvendy.spark.day01

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TestList {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("hello")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("aa", 1), ("aa", 2), ("bb", 3), ("bb", 4)))
    val groupByKey = rdd1.groupByKey();
    println("groupByKey")
    for (l <- groupByKey) {
      println(l)
    }

    /**
     * union (类型一致，可以合并)
     */
    println("union")
    val rdd2 = sc.parallelize(List(("d", 5), ("dd", 6)))
    var newRdd = rdd1.union(rdd2);
    val list = newRdd.collect();
    for (l <- list) {
      println(l)
    }

    /**
     *   join
     */
    println("rddtest12.....")
    val rddtest1 = sc.parallelize(List(("James", 1), ("Wade", 2), ("Paul", 3)))
    val rddtest2 = sc.parallelize(List(("James", 4), ("Wade", 5)))
    val rddtest12 = rddtest1 join rddtest2 // 不加点换空格
    for (l <- rddtest12.collect()) {
      println(l);
    }
    
    /**
     * intersection
     */
   /* val rddintersection1 = sc.parallelize(List(1,2,3))
    val rddintersection2 = sc.parallelize(List(1,2)
    val rddintersection =  rddtest1.intersection(rddtest2)
    println("rddintersection:"+rddintersection.collect().mkString(","))*/
    
    //distinct
    
    /**
     * cogroup
     */
    println("cogroup....")
    val rddCogroup = rddtest1 cogroup rddtest2
    for (l <- rddCogroup.collect()) {
      println(l);
    }

    /**
     *  foreach(jvm)
     */
    var counter = 0
    var rdd = sc.parallelize(List(1, 2, 3, 2, 1, 4, 5, 7, 9))
    // Wrong: Don't do this!!
    /* rdd.foreach(x => counter += x)
    println("Counter value: " + counter)*/
    /**
     * To ensure well-defined behavior in these sorts of scenarios one should use an Accumulator.
     *  Accumulators in Spark are used specifically to provide a mechanism for safely updating
     *  a variable when execution is split up across worker nodes in a cluster.
     *  The Accumulators section of this guide discusses these in more detail.
     */

    println("rdd-count:" + rdd.count());
    /**
     *  reduceRdd (累加)，不要用foreach
     */
    val reduceRdd = sc.parallelize(List(1, 2, 3, 4, 5))
    println(reduceRdd.reduce(_ + _))

    /**
     *  countByKey
     *
     */
    val rddCountByKey = sc.parallelize(List(("a", 1), ("a", 2), ("b", 2), ("c", 2), ("bb", 4)))
    println("countByKey:" + rddCountByKey.countByKey())
    println("countByValue:" + rddCountByKey.countByValue())

    /**
     * lookup(通过key,获取值)
     *
     */
    println("loolup:")
    val loolup = rddCountByKey.lookup("a");
    for (l <- loolup.seq) {
      println(l)
    }

    sc.stop()
  }
}