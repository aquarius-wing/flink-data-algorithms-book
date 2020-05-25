package io.miaoji.flink.algorithms.scala.chap09

import io.miaoji.flink.algorithms.scala.chap07.TestData
import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * 包: io.miaoji.flink.algorithms.scala.chap08
 * 开发者: wing
 * 开发时间: 2019-05-25
 * 功能：共同好友
 *
 */
object FindCommonFriends {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = TestData.getData(env)
  }

}
