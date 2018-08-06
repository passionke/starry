package com.github.passionke.replace

import com.github.passionke.starry.SparkPlanExecutor
import com.github.passionke.baseline.Dumy
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, SubqueryAlias}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.Spark
import org.scalatest.FunSuite

/**
  * Created by passionke on 2018/6/16.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
class ReplaceGroup extends FunSuite {

  test("group by") {
    val sparkSession = Spark.sparkSession
    sparkSession.sparkContext.setLogLevel("WARN")
    import sparkSession.implicits._
    val dumys = Seq(Dumy("a", 10, "abc"), Dumy("a", 20, "ass"))
    dumys.toDF().createOrReplaceTempView("a")

    val df = sparkSession.sql(
      """
        |select name, count(1) as cnt
        |from a
        |group by name
      """.stripMargin)

    df.show()
    val sparkPlan = df.queryExecution.sparkPlan
    val logicalPlan = df.queryExecution.analyzed


    val dumy1 = Seq(Dumy("a", 1, "abc"), Dumy("a", 1, "ass"), Dumy("a", 2, "sf"))
    val data = dumy1.toDF().queryExecution.executedPlan.execute().collect()

    val newL = logicalPlan.transform({
      case SubqueryAlias(a, localRelation) if a.equals("a") =>
        SubqueryAlias(a, LocalRelation(localRelation.output, data))
    })

    val ns = sparkSession.newSession()
    val qe = new QueryExecution(ns, newL)
    val start = System.currentTimeMillis()
    val list = SparkPlanExecutor.exec(qe.sparkPlan, ns)
    assert(list.head.getLong(1).equals(3L))
    val end = System.currentTimeMillis()
    end - start
  }

}
