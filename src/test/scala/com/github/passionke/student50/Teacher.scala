package com.github.passionke.student50

/**
  * Created by passionke on 2018/7/12.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
case class Teacher(id: String, name: String) {

}

object Teacher {
  private val database =
    """
      |insert into Teacher values('01' , '张三')
      |insert into Teacher values('02' , '李四')
      |insert into Teacher values('03' , '王五')
    """.stripMargin

  def teachers(): List[Teacher] = {
    database.split("\n").map(parse).filter(p => p.isDefined).map(p => p.get).toList
  }

  def parse(str: String): Option[Teacher] = {
    val words = str.trim.replaceAll("insert into Teacher values\\(", "").replaceAll("\\);", "")
      .split(",")
      .map(word => {
        word.replaceAll("'", "").trim
      })
    if (words.length == 2) {
      Some(Teacher(words.apply(0), words.apply(1)))
    } else {
      None
    }
  }
}