package io.miaoji.flink.algorithms.scala.chap08

import org.apache.flink.api.scala._

/**
  * 包: io.miaoji.flink.algorithms.scala.chap08
  * 开发者: wing
  * 开发时间: 2019-02-24
  * 功能：
  */
object TestData {

  def getData(env: ExecutionEnvironment) = {
    env.fromElements(
      Array("crackers", "bread", "banana"),
      Array("crackers", "coke", "butter", "coffee"),
      Array("crackers", "bread"),
      Array("crackers", "bread"),
      Array("crackers", "bread", "coffee"),
      Array("butter", "coke"),
      Array("butter", "coke", "bread", "crackers")
    )
  }

}
