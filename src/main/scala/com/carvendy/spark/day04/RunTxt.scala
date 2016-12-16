package com.carvendy.spark.day04

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Run01 {
  
  
  def main(args: Array[String]): Unit = {
    
     var start = System.currentTimeMillis()
     val conf = new SparkConf()
      conf.setMaster("local")
          .setAppName("hello")
      val sc = new SparkContext(conf)
     
     var  txtRdd = sc.textFile("/tmp/test-spark-data2.txt")
     var end = System.currentTimeMillis()
     println("lines:"+txtRdd.count()+",read-time:"+(end-start))
     
     start = System.currentTimeMillis()
     var  rdd = txtRdd.map ( x => x.split(",") )
       .filter { x => x.contains("a") };
      end = System.currentTimeMillis()
      println("a-count:"+rdd.count()+",read-time:"+(end-start))
      
     sc.stop()
  }
  

}