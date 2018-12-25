package io.miaoji.flink.algorithms.scala.chap03

import org.apache.flink.api.scala.ExecutionEnvironment
;

/**
  * 使用了sortPartition
  */
object Top10{

  val INPUT_FILE_NAME = "chap03/top10_input.txt"
  val OUTPUT_FILE_NAME = "chap03/top10_output.txt"

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment



    env.execute("top 10")
  }
}
