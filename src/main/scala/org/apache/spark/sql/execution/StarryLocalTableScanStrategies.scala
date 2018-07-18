package org.apache.spark.sql.execution

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.logical.StarryLocalRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * Created by passionke on 2018/7/18.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
case class StarryLocalTableScanStrategies() extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case StarryLocalRelation(tableName, output, data, _) =>
      StarryLocalTableScanExec(tableName, output, data) :: Nil
    case _ => Nil
  }
}
