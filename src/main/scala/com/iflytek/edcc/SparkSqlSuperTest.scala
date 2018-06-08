package com.iflytek.edcc

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/1/17
  * Time: 16:15
  * Description
  */
//主要学习使用高级的dataframe和datasets的api,
//并了解spark的Catalyst和Tungsten两个组件
object SparkSqlSuperTest {

  /**
    * 语句流程：
  SQL语句
    --> 调用sqlparse --> unresolved logical plan
    --> 调用analyzer --> resovled logical plan
    --> 调用optimizer --> optimized logical plan
    --> 调用sparkPlanner --> sparkPlan
    --> 调用prepareForExecution --> prepared sparkplan

    * 执行流程：
  sparkplan --> 调用execute --> RDD
  --> 调用converter --> Scala数据
    */

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName(this.getClass.getName)
    conf.setMaster("local[4]")

    //spark程序入口，负责与集群交互
    val sc = new SparkContext(conf)
    //带schema的结构化数据集合处理的程序入口
    val sqlContext = new SQLContext(sc)

    val dataframe = sqlContext.read.parquet("D:\\project\\edu_edcc\\ztwu2\\data\\parquet")
    dataframe.registerTempTable("test")
    val result = sqlContext.sql("select * from test")
    result.foreach(println)

    //从技术角度，越底层和硬件偶尔越高，可动弹的空间越小，而越高层，可动用的智慧是更多。Catalyst就是个高层的智慧。
    //Catalyst已经逐渐变成了所有Spark框架的解析优化引擎，
    // RDD是通用抽象的数据结果，RDD+Catalyst就构成了Spark的新底层。
    // Catalyst是在RDD基础上进行封装，一旦优化了Catalyst，所有的子框架就都得到了优化。
    println("打印执行计划")
    println(result.queryExecution)

    sc.stop()

  }

}
