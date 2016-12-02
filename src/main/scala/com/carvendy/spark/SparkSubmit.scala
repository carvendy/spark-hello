package com.carvendy.spark

import org.apache.spark.SparkConf
import java.util.ArrayList
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkSubmit {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf();
    conf.setAppName("alleyz-lad").setMaster("spark://kvm-5-118:7077");
    val sc = new SparkContext(conf);
    val path = "D:\\workspace\\spark-hello\\target\\spark-hello-0.0.1-jar-with-dependencies.jar"
   // jsc.addJar(path);  //添加依赖的jar，不是远程提交并执行
   
    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)
    println(distData)
    
    sc.stop();
  }

}