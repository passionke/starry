package org.apache.spark.sql.execution

import com.github.passionke.starry.SparkPlanExecutor
import org.apache.spark.rdd.{RDD, StarryRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}

/**
  * Created by passionke on 2018/6/27.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
case class StarryCollectLimitExec(limit: Int, child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = SinglePartition

  protected override def doExecute(): RDD[InternalRow] = {
    val childLimit = SparkPlanExecutor.doExec(child).take(limit)
    new StarryRDD[InternalRow](sparkContext, childLimit)
  }
}