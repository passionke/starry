package com.github.passionke.baseline

import com.github.passionke.ProfileUtils
import com.github.passionke.starry.SparkPlanExecutor
import org.apache.spark.Spark
import org.scalatest.FunSuite

/**
  * Created by passionke on 2018/6/4.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
class WindowFucTest extends FunSuite {

  test("window") {
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
    val sparkPlan = df.queryExecution.sparkPlan
    ProfileUtils.profileMuti(() => {
      val start = System.currentTimeMillis()
      val list = SparkPlanExecutor.exec(sparkPlan, sparkSession)
      assert(list.head.getLong(1).equals(1L))
      assert(list.apply(1).getLong(1).equals(1L))
      val end = System.currentTimeMillis()
      end - start
    }, 1000, 1)
  }
}
