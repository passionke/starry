package com.github.passionke.starry

import org.apache.spark.util.StarryClosureCleaner
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by passionke on 2018/6/27.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
class StarrySparkContext(sparkConf: SparkConf) extends SparkContext(sparkConf) {

  override def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
    StarryClosureCleaner.clean(f, checkSerializable)
    f
  }

}
