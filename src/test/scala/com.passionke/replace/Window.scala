package com.passionke.replace

import com.passionke.baseline.Dumy
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, SubqueryAlias}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.{Spark, SparkPlanExecutor}
import org.scalatest.FunSuite

/**
  * Created by passionke on 2018/6/16.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
class Window extends FunSuite {

  test("window by") {
    val sparkSession = Spark.sparkSession
    sparkSession.sparkContext.setLogLevel("WARN")
    import sparkSession.implicits._
    val dumys = Seq(Dumy("a", 10, "abc"), Dumy("a", 20, "add"))
    dumys.toDF().createOrReplaceTempView("a")

    val df = sparkSession.sql(
      """
        |select name, count(1) over(partition by age) as cnt
        |from a
      """.stripMargin)

    df.show()

    val dumy1 = Seq(Dumy("a", 10, "abc"), Dumy("a", 20, "add"), Dumy("a", 10, "sf"))

    val data = dumy1.toDF().queryExecution.executedPlan.execute().collect()

   val logicalPlan = df.queryExecution.analyzed.transform({
      case SubqueryAlias(a, lcr) if a.equals("a") =>
        SubqueryAlias(a, LocalRelation(lcr.output, data))
    })

//    DatasetUtil.toDf(sparkSession, logicalPlan).show()

    val qe = new QueryExecution(sparkSession, logicalPlan)

    val list = SparkPlanExecutor.exec(qe.sparkPlan, sparkSession)

    assert(list.head.getLong(1).equals(2L))
    assert(list.apply(1).getLong(1).equals(2L))
  }

}
