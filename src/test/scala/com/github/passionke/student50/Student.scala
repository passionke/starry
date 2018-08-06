package com.github.passionke.student50

import java.util.Date

import org.apache.commons.lang3.time.DateUtils

/**
  * Created by passionke on 2018/7/12.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
case class Student(id: String, name: String, age: String, sex: String) {

}


object Student {

  val database: String =
    """
      |insert into Student values('01' , '赵雷' , '1990-01-01' , '男');
      |insert into Student values('02' , '钱电' , '1990-12-21' , '男');
      |insert into Student values('03' , '孙风' , '1990-05-20' , '男');
      |insert into Student values('04' , '李云' , '1990-08-06' , '男');
      |insert into Student values('05' , '周梅' , '1991-12-01' , '女');
      |insert into Student values('06' , '吴兰' , '1992-03-01' , '女');
      |insert into Student values('07' , '郑竹' , '1989-07-01' , '女');
      |insert into Student values('09' , '张三' , '2017-12-20' , '女');
      |insert into Student values('10' , '李四' , '2017-12-25' , '女');
      |insert into Student values('11' , '李四' , '2017-12-30' , '女');
      |insert into Student values('12' , '赵六' , '2017-01-01' , '女');
      |insert into Student values('13' , '孙七' , '2018-01-01' , '女');
    """.stripMargin

  def students(): List[Student] = {
    database.split("\n").map(parse).filter(p => p.isDefined).map(p => p.get).toList
  }

  def parse(str: String): Option[Student] = {
    val words = str.trim.replaceAll("insert into Student values\\(", "").replaceAll("\\);", "")
      .split(",")
      .map(word => {
        word.replaceAll("'", "").trim
      })
    if (words.length == 4) {
      Some(Student(words.apply(0), words.apply(1), words.apply(2), words.apply(3)))
    } else {
      None
    }
  }
}