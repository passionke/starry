package org.apache.spark.sql

import org.apache.spark.Spark.ExtensionsBuilder
import org.apache.spark.sql.catalyst.optimizer.StarryLocalRelationReplace

/**
  * Created by passionke on 2018/7/18.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */

class StarrySparkSessionExtension extends ExtensionsBuilder {

  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {
    sparkSessionExtensions.injectOptimizerRule(_ =>
      StarryLocalRelationReplace)
  }
}


