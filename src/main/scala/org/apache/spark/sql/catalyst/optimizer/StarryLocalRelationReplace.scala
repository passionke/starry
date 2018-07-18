package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.logical.StarryLocalRelation
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule

/**
  * Created by passionke on 2018/6/28.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
object StarryLocalRelationReplace extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case SubqueryAlias(alias, child) if child.isInstanceOf[LocalRelation] =>
      val c = child.asInstanceOf[LocalRelation]
      SubqueryAlias(alias, StarryLocalRelation(alias, c.output, c.data))
  }
}
