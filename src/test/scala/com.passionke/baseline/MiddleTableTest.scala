package com.passionke.baseline

import org.apache.spark.{Spark, SparkPlanExecutor}
import org.scalatest.FunSuite

/**
  * Created by passionke on 2018/6/14.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
class MiddleTableTest extends FunSuite {

  test("middle") {
    val sparkSession = Spark.sparkSession
    sparkSession.sparkContext.setLogLevel("WARN")
    import sparkSession.implicits._
    val dumys = Seq(Dumy("a", 10, "abc"), Dumy("a", 20, "aaa"), Dumy("a", 30, "ccc"))
    dumys.toDF().createOrReplaceTempView("a")

    val d = sparkSession.sql("select * from a where age < 20")
    d.createOrReplaceTempView("d")

    val e = sparkSession.sql("select max(age) from d")
    val list = SparkPlanExecutor.exec(e.queryExecution.sparkPlan, sparkSession)
    list.foreach(println)
  }
}

