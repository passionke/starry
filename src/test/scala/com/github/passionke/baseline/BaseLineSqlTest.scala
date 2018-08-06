package com.github.passionke.baseline

import com.github.passionke.ProfileUtils
import com.github.passionke.starry.SparkPlanExecutor
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.execution.LocalBasedStrategies
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.Spark
import org.scalatest.FunSuite
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by passionke on 2018/6/1.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
class BaseLineSqlTest extends FunSuite {

  val log: Logger = LoggerFactory.getLogger(classOf[BaseLineSqlTest])

  test("filter") {
    val sparkSession = Spark.sparkSession
    sparkSession.sparkContext.setLogLevel("WARN")
    import sparkSession.implicits._
    val dumys = Seq(Dumy("a", 10, "abc"))
    val dataFrame = dumys.toDF()

    dataFrame.createOrReplaceTempView("dumy")

    sparkSession.sql("select * from dumy where age < 20").show()

    val df = sparkSession.sql("select * from dumy where age < 20")

    val sparkPlan = df.queryExecution.sparkPlan
    ProfileUtils.profileMuti(() => {
      val start = System.currentTimeMillis()
      val list = SparkPlanExecutor.exec(sparkPlan, sparkSession)
      assert(list.head.getString(0).equals("a"))
      val end = System.currentTimeMillis()
      end - start
    }, 10000, 1)
  }

  test("filter complex") {
    val sparkSession = Spark.sparkSession
    sparkSession.sparkContext.setLogLevel("WARN")
    loadParquetToTable(sparkSession, "./target/test-classes/ods_loan_consumer_application_cell_call_mob", "ods_loan_consumer_application_cell_call_mob")
    val d = sparkSession.sql(
      """
        |select
        |uid, mob
        |from ods_loan_consumer_application_cell_call_mob c
        |where
        |       (c.apply_status in (1,2) or (c.apply_status in (-1) and c.approve_status not in (0)))
        |       and (c.approve_reject_code is null or c.approve_reject_code not in ('application message is timeout'))
        |       and datediff('2018-05-24 10:45:30',c.apply_time) <=7 and c.apply_time < '2018-05-24 10:45:30'
        |       and c.uid != '88990337' and c.approve_model_status = 3
      """.stripMargin)

    d.show()
    val converters = CatalystTypeConverters.createToScalaConverter(d.schema)
    d.queryExecution.sparkPlan.execute().collect().foreach(f => println(f.toString))
    val s = d.queryExecution.sparkPlan.execute().map(f => converters.apply(f).asInstanceOf[Row])
    val b = s.collect()
    b.foreach(println)
    assert(b.head.getString(1).equals("13296811395"))
  }

  def loadParquetToTable(sparkSession: SparkSession, tablePath: String, tableName: String): Unit = {
    val df = sparkSession.read.load(tablePath)
    LocalBasedStrategies.unRegister(sparkSession)

    val rows = df.collectAsList()
    LocalBasedStrategies.register(sparkSession)
    sparkSession.createDataFrame(rows, df.schema).createOrReplaceTempView(tableName)

  }

  // 暂时不zhichi parquet格式的计算
  test("filter complex in executor") {
    val sparkSession = Spark.sparkSession
    sparkSession.sparkContext.setLogLevel("WARN")
    loadParquetToTable(sparkSession, "./target/test-classes/ods_loan_consumer_application_cell_call_mob", "ods_loan_consumer_application_cell_call_mob")
    val d = sparkSession.sql(
      """
        |select
        |uid, mob
        |from ods_loan_consumer_application_cell_call_mob c
        |where
        |       (c.apply_status in (1,2) or (c.apply_status in (-1) and c.approve_status not in (0)))
        |       and (c.approve_reject_code is null or c.approve_reject_code not in ('application message is timeout'))
        |       and datediff('2018-05-24 10:45:30',c.apply_time) <=7 and c.apply_time < '2018-05-24 10:45:30'
        |       and c.uid != '88990337' and c.approve_model_status = 3
      """.stripMargin)

    d.show()
    val b = SparkPlanExecutor.exec(d.queryExecution.sparkPlan, sparkSession)
    println(b)
    assert(b.head.getString(1).equals("13296811395"))
  }
}

