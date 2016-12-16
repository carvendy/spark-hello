package com.carvendy.spark.day03

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TestException {
 
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf()
    conf.setMaster("local").setAppName("hello")
    val sc = new SparkContext(conf)
    sc.parallelize(0 until 10).foreach { i =>
      println(i)
      if (math.random > 0.75) {
        throw new Exception("Testing exception handling")
      }
    }
  }
}