package io.miaoji.flink.algorithms.scala.chap03

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
;

/**
  * 使用了sortPartition
  */
object Top10{

  val INPUT_FILE_NAME = "chap03/top10_input.txt"
  val OUTPUT_FILE_NAME = "chap03/top10_output.txt"

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val dir = this.getClass.getClassLoader.getResource("")
    val inputFilePath = dir + "/" + INPUT_FILE_NAME
    val outputFilePath = dir + "/" + OUTPUT_FILE_NAME
    val dataSource = env.readTextFile(inputFilePath)
    dataSource
      // STEP-1: 用逗号进行分隔
      .map[Array[String]] { x: String => x.split(",") }
      // STEP-2: 过滤不符合格式的参数
      .filter(_.length == 2)
      // STEP-3: 第一个是猫id，第二个是猫体重
      .map { x: Array[String] => (x(0), x(1).toInt, 1) }
      // STEP-4: 将所有数据划为一个分区
      .partitionByRange(2)
      // STEP-5: 对同一分区内的数据的第1列（下表从0开始）进行排序
      .sortPartition(1, Order.DESCENDING)
      // STEP-6: 输出到文件
      .writeAsText(OUTPUT_FILE_NAME, WriteMode.OVERWRITE).setParallelism(1)

    env.execute("top 10")
  }
}
