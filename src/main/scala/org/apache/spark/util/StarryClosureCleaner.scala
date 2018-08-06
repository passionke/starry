package org.apache.spark.util

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.{SparkEnv, SparkException}

import scala.collection.mutable

/**
  * Created by passionke on 2018/6/8.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
object StarryClosureCleaner extends Logging {

  val serializableMap: LRUCache[String, Boolean] = new LRUCache[String, Boolean](100000)

  // Check whether a class represents a Scala closure
  private def isClosure(cls: Class[_]): Boolean = {
    cls.getName.contains("$anonfun$")
  }

  def clean(
             closure: AnyRef,
             checkSerializable: Boolean = true,
             cleanTransitively: Boolean = true): Unit = {
    clean(closure, checkSerializable, cleanTransitively, mutable.Map.empty)
  }

  private def clean(
                     func: AnyRef,
                     checkSerializable: Boolean,
                     cleanTransitively: Boolean,
                     accessedFields: mutable.Map[Class[_], mutable.Set[String]]): Unit = {

    if (!isClosure(func.getClass)) {
      logWarning("Expected a closure; got " + func.getClass.getName)
      return
    }
    if (func == null) {
      return
    }
    if (checkSerializable) {
      ensureSerializable(func)
    }
  }

  private def ensureSerializable(func: AnyRef) {
    if (!serializableMap.containsKey(func.getClass.getCanonicalName)) {
      try {
        if (SparkEnv.get != null) {
          SparkEnv.get.closureSerializer.newInstance().serialize(func)
          serializableMap.put(func.getClass.getCanonicalName, true)
        }
      } catch {
        case ex: Exception => throw new SparkException("Task not serializable", ex)
      }
    }
  }

  case class LRUCache[K, V](cacheSize: Int) extends util.LinkedHashMap[K, V] {

    override def removeEldestEntry(eldest: util.Map.Entry[K, V]): Boolean = size > cacheSize

  }

}

