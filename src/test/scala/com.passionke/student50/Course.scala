package com.passionke.student50

/**
  * Created by passionke on 2018/7/12.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
case class Course(id: String, name: String, tid: String) {

}

object Course {

  private val database: String =
    """
      |insert into Course values('01' , '语文' , '02')
      |insert into Course values('02' , '数学' , '01')
      |insert into Course values('03' , '英语' , '03')
    """.stripMargin

  def courses(): List[Course] = {
    database.split("\n").map(parse).filter(p => p.isDefined).map(p => p.get).toList
  }

  def parse(str: String): Option[Course] = {
    val words = str.trim.replaceAll("insert into Course values\\(", "").replaceAll("\\);", "")
      .split(",")
      .map(word => {
        word.replaceAll("'", "").trim
      })
    if (words.length == 3) {
      Some(Course(words.apply(0), words.apply(1), words.apply(2)))
    } else {
      None
    }
  }


}