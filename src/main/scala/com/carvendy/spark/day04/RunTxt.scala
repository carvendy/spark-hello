package com.carvendy.spark.day04

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object RunTxt {
  
  def main(args: Array[String]): Unit = {
    var num = 1
    if(args.length > 0){
      num = args(0).toInt
    }
    
     var start = System.currentTimeMillis()
     val conf = new SparkConf()
      //conf.setMaster("local[*]")
          .setAppName("hello")
          //.set("spark.driver.maxResultSize", "4g")
      val sc = new SparkContext(conf)
     
     val  txtRdd = sc.textFile("hdfs://kvm-5-118:9000/tmp/test-spark-data"+num+".txt")
    // val txtRdd2 = sc.parallelize(txtRdd.collect(), 2)
     
     var end = System.currentTimeMillis()
     var readTime = end-start
     start = System.currentTimeMillis()
     var  rdd = txtRdd.flatMap ( x => x.split(",") )
       .filter { x => x.contains("a") }
     var lines = txtRdd.count() 
     var count = rdd.count()
     sc.stop()
     
     end = System.currentTimeMillis()
     println("lines:"+lines+",time:"+readTime)
     println("a-count:"+count+",time:"+(end-start))
  }
  

}