package com.carvendy.spark.day01

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.File
import scala.reflect.io.File

object TestCount {
  
  val filePath = "count.txt";
  
  
  /**
   * 
   * http://spark.apache.org/docs/latest/programming-guide.html#transformations
   * 
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
   // conf.setMaster("spark://kvm-5-118:7077")
    conf.setMaster("local")
    .setAppName("hello")
    val sc = new SparkContext(conf)
    
    /**
     *   从集合中取数据
     */
    /*val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input.map(x => x * x)
    println(result.collect().mkString(","))*/
    
    /**
     *  从文件中取数据
     */
    val url = TestCount.getClass.getClassLoader.getResource(filePath);
    val textFile = sc.textFile(url.getFile)
    val temp = textFile.flatMap(line => line.split("\\s+"))
    /**
     * flatMap 键相同并存
     */
    println("flatMap:"+temp.collect().mkString("#"))
    
    val result = textFile.flatMap(line => line.split("\\s+"))
        .map(word => (word, 1))
        //.filter(2)
        //.filterByRange("b", "c")
        .reduceByKey(_ + _) // 合并数据
         .map(x=>(x._2, x._1))// 键值互换
         .sortByKey(false)// true 为升序,fals为降序
         .map(x=>(x._2,x._1))
         
    println(result.collect().mkString(","))
    //result.saveAsTextFile("d:/data_new")
   /**
    *  saveAsObjectFile(序列化)
    */
    
   /* var list =  result.collect()
    for(l <- list){
      println(l)
    }*/
    
    sc.stop();
  }
}