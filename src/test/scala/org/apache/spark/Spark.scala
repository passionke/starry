package org.apache.spark

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.execution.LocalBasedStrategies

/**
  * Created by passionke on 2018/6/27.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
object Spark {

  val sparkConf = new SparkConf()
  sparkConf.setMaster("local[*]")
  sparkConf.setAppName("aloha")
  sparkConf
    .set("spark.default.parallelism", "1")
    .set("spark.sql.shuffle.partitions", "1")
    .set("spark.broadcast.manager", "rotary")
    .set("rotary.shuffer", "true")
    .set("spark.sql.codegen.wholeStage", "false")
    .set("spark.sql.extensions", "org.apache.spark.sql.StarrySparkSessionExtension")
  val sparkContext = new StarrySparkContext(sparkConf)
  val sparkSession: SparkSession =
    SparkSession.builder
      .sparkContext(sparkContext)
      .getOrCreate

  LocalBasedStrategies.register(sparkSession)
}
