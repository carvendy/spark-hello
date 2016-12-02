package com.carvendy.spark.day01

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TestList {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("hello")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("aa", 1), ("aa", 2), ("bb", 3), ("bb", 4)))
    val  haha = rdd1.groupByKey();
    println(haha)
    
    
    var counter = 0
    var rdd = sc.parallelize(List(1,2,3,2,1,4,5,7,9))
    // Wrong: Don't do this!!
   /* rdd.foreach(x => counter += x)
    println("Counter value: " + counter)*/
    
  }
}