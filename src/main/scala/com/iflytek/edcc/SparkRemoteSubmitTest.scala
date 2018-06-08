package com.iflytek.edcc

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/3/28
  * Time: 10:13
  * Description
  */

object SparkRemoteSubmitTest {

 def main(args:Array[String]):Unit = {
  val conf = new SparkConf()
  conf.setMaster("spark://192.168.1.101:7077")
//  conf.setMaster("local[4]")
  conf.setAppName(this.getClass.getName)
  conf.set("spark.hadoop.validateOutputSpecs","false")

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val rdd = sc.makeRDD(Seq(1,2,3,4,5))
  rdd.foreach(x=>println(x))

//  rdd.saveAsTextFile("D:\\project\\edu_edcc\\ztwu2\\spark-test\\data\\")
//  val dataframe = rdd.toDF("id")
//  dataframe.write.format("parquet").mode(SaveMode.Overwrite).save("D:\\project\\edu_edcc\\ztwu2\\spark-test\\data1\\")

  System.out.println(rdd.getNumPartitions)
  System.out.println(rdd.partitions.size)
  val partitions = rdd.partitions;
  System.out.println(rdd.preferredLocations(partitions.apply(0)))

 }

}
