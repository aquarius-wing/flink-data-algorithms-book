package io.miaoji.flink.algorithms.scala.chap09

import org.apache.flink.api.scala._

/**
  * 包: io.miaoji.flink.algorithms.scala.chap09
  * 开发者: wing
  * 开发时间: 2019-05-25
  * 功能：
  */
object TestData {

  def getData(env: ExecutionEnvironment) = {
    env.fromElements(
      ("u1", "i1,i2,i3"),
      ("u2", "i2,i3"),
      ("u3", "i2,i3,i4"),
      ("u4", "i5,i6"),
      ("u5", "i3,i4")
    )
  }

}
