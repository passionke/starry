package com.github.passionke.baseline

import com.github.passionke.ProfileUtils
import com.github.passionke.starry.SparkPlanExecutor
import org.apache.spark.Spark
import org.scalatest.FunSuite

/**
  * Created by passionke on 2018/6/5.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
class UionTest extends FunSuite {

  test("uion") {
    val sparkSession = Spark.sparkSession
    sparkSession.sparkContext.setLogLevel("WARN")
    import sparkSession.implicits._
    val dumys = Seq(Dumy("a", 10, "abc"))
    val dataFrame = dumys.toDF()

    dataFrame.createOrReplaceTempView("dumy")

    val sql = "select * from dumy where age < 20 union all select * from dumy where age < 5"
    sparkSession.sql(sql).show()

    val df = sparkSession.sql(sql)

    val sparkPlan = df.queryExecution.sparkPlan
    ProfileUtils.profileMuti(() => {
      val start = System.currentTimeMillis()
      val list = SparkPlanExecutor.exec(sparkPlan, sparkSession)
      assert(list.head.getString(0).equals("a"))
      val end = System.currentTimeMillis()
      end - start
    }, 100000, 1)
  }
}
