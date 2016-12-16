package com.carvendy.spark.day02

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TestFun {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("hello")
    val sc = new SparkContext(conf)
    
    /**
     * longAccumulator
     */
    val accum = sc.longAccumulator("My Accumulator")
    sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum.add(x))
    println("accum:" + accum.value)
    
    /*val  serializerType=  conf.get("spark.serializer");
    println("KryoSerializer"+serializerType)*/
    
    var array = 1 until 10// 没到10
    //array.foreach { i => println(i) }
    
    val rdd = sc.parallelize(1 until 20, 10);//numSlices 分片
    rdd.collect().foreach { i => println(i) }
  }
}