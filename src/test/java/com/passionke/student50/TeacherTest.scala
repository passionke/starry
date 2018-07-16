package com.passionke.student50

import org.scalatest.FunSuite

/**
  * Created by passionke on 2018/7/12.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
class TeacherTest extends FunSuite {

  test("testTeachers") {
    val ts = Teacher.teachers()
    ts.foreach(println)
    assert(ts.head.id.equals("01"))
  }

}
