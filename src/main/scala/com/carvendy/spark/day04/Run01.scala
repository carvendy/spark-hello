package com.carvendy.spark.day04

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Run01 {
  
  
  def main(args: Array[String]): Unit = {
    
     val conf = new SparkConf()
      conf.setMaster("local").setAppName("hello")
      val sc = new SparkContext(conf)
  }
}