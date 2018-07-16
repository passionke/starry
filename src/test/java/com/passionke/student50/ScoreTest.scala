package com.passionke.student50

import org.scalatest.FunSuite

/**
  * Created by passionke on 2018/7/12.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
class ScoreTest extends FunSuite {

  test("testScores") {
    val ss = Score.scores()
    ss.foreach(println)
    assert(ss.head.sId.equals("01"))
  }

}
