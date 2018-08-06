package com.github.passionke.student50

import org.scalatest.FunSuite

/**
  * Created by passionke on 2018/7/12.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
class StudentTest extends FunSuite {

  test("testStudents") {
    val sts = Student.students()
    sts.foreach(println)
    assert(sts.apply(1).name.equals("钱电"))
  }

}
