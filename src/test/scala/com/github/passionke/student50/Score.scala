package com.github.passionke.student50

import java.math.MathContext

/**
  * Created by passionke on 2018/7/12.
  * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
  * 一朝鹏程，快意风云，挥手功名
  */
case class Score(sId: String, cId: String, score: Long) {
}

object Score {
  private val database =
    """
      |insert into SC values('01' , '01' , 80);
      |insert into SC values('01' , '02' , 90);
      |insert into SC values('01' , '03' , 99);
      |insert into SC values('02' , '01' , 70);
      |insert into SC values('02' , '02' , 60);
      |insert into SC values('02' , '03' , 80);
      |insert into SC values('03' , '01' , 80);
      |insert into SC values('03' , '02' , 80);
      |insert into SC values('03' , '03' , 80);
      |insert into SC values('04' , '01' , 50);
      |insert into SC values('04' , '02' , 30);
      |insert into SC values('04' , '03' , 20);
      |insert into SC values('05' , '01' , 76);
      |insert into SC values('05' , '02' , 87);
      |insert into SC values('06' , '01' , 31);
      |insert into SC values('06' , '03' , 34);
      |insert into SC values('07' , '02' , 89);
      |insert into SC values('07' , '03' , 98);
    """.stripMargin

  def scores(): List[Score] = {
    database.split("\n").map(parse).filter(p => p.isDefined).map(p => p.get).toList
  }

  def parse(str: String): Option[Score] = {
    val words = str.trim.replaceAll("insert into SC values\\(", "").replaceAll("\\);", "")
      .split(",")
      .map(word => {
        word.replaceAll("'", "").trim
      })
    if (words.length == 3) {
      Some(Score(words.apply(0), words.apply(1), words.apply(2).toLong))
    } else {
      None
    }
  }
}
