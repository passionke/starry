package com.passionke.baseline

import org.apache.spark.sql.execution.StarryCollectLimitExec
import org.apache.spark.{Spark, SparkPlanExecutor}
import org.scalatest.FunSuite

/**
  * Created by passionke on 2018/6/22.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
class LimitTest extends FunSuite {

  test("limit") {
    val sparkSession = Spark.sparkSession
    sparkSession.sparkContext.setLogLevel("WARN")
    import sparkSession.implicits._
    val dumys = Seq(Dumy("a", 1, "abc"), Dumy("a", 2, "abc"), Dumy("a", 3, "abc"))
    dumys.toDF().createOrReplaceTempView("a")

    val df = sparkSession.sql("select age from a where age > 0 limit 1")
    val list = SparkPlanExecutor.exec(df.queryExecution.sparkPlan, sparkSession)
    assert(df.queryExecution.sparkPlan.find(f => f.isInstanceOf[StarryCollectLimitExec]).isDefined)
    assert(list.head.getInt(0).equals(1))
  }

}
