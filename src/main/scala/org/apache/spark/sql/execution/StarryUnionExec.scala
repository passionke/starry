package org.apache.spark.sql.execution

import com.github.passionke.starry.SparkPlanExecutor
import org.apache.spark.rdd.{RDD, StarryRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute

/**
  * Created by passionke on 2018/6/27.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
case class StarryUnionExec(children: Seq[SparkPlan]) extends SparkPlan {
  override def output: Seq[Attribute] =
    children.map(_.output).transpose.map(attrs =>
      attrs.head.withNullability(attrs.exists(_.nullable)))

  protected override def doExecute(): RDD[InternalRow] = {
    val b = children.flatMap(child => {
      SparkPlanExecutor.doExec(child)
    })
    new StarryRDD(sparkContext, b)
  }

}