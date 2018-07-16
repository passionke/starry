package com.passionke.baseline

import java.util.concurrent.{Callable, Executors, Future}

import org.apache.spark.internal.Logging

/**
  * Created by passionke on 2018/5/21.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
object ProfileUtils extends Logging {

  def profileMuti(f: () => Long, times: Int, threads: Int): Unit = {
    val executorService = Executors.newFixedThreadPool(threads)

    val b: IndexedSeq[Future[Long]] = 1.to(times).map(i => {
      executorService.submit(new Callable[Long] {
        override def call(): Long = {
          try {
            val t = f.apply()
            t
          } catch {
            case e: Exception => {}
              log.error("opps", e)
              0
          }
        }
      })
    })
    val t = b.map(task => task.get())
    println("meanTime " + mean(removeOuterlier(t)))
  }

  import Numeric.Implicits._

  def mean[T: Numeric](xs: Iterable[T]): Double = {
    xs.sum.toDouble() / xs.size
  }

  def removeOuterlier[T: Numeric](xs: Iterable[T]): Iterable[T] = {
    val std = stdV(xs)
    val m = mean(xs)
    xs.filter(p => Math.abs(p.toDouble() - m) < 1 * std)
  }

  def variance[T: Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)

    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }

  def stdV[T: Numeric](xs: Iterable[T]): Double = {
    Math.sqrt(variance(xs))
  }
}
