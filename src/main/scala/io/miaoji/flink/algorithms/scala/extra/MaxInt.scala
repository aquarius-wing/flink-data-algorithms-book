package io.miaoji.flink.algorithms.scala.extra

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger

/**
  * 当前用户 : wing。
  * 当前日期 : 2018/12/9。
  * 包名 : io.miaoji.flink.algorithms.scala.extra。
  * 当前时间 : 11:18 PM。
  */
object MaxInt {

  val INPUT_FILE_NAME = "extra/max_int_input.txt"
  val OUTPUT_FILE_NAME = "extra/max_int_output.txt"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dir = MaxInt.getClass.getClassLoader.getResource("")

    val inputFilePath = dir + "/" + INPUT_FILE_NAME
    val outputFilePath = dir + "/" + OUTPUT_FILE_NAME
    val interval:Long=100L
    val fileStream = env.readFile(new TextInputFormat(new Path(inputFilePath)), inputFilePath, FileProcessingMode.PROCESS_CONTINUOUSLY, interval)

    fileStream
      // 120 3aW
      // 按空格分割然后打平
      .flatMap(_.split(" "))
      // 120或者3aw
      // 只要数字
      .filter((str: String) =>
        str.matches("[0-9]{3}")
      )
      // 加1是为了把数字变成元祖
      .map((str: String) =>
        ("1",str.toInt)
      )
      .keyBy(0)
      .windowAll(GlobalWindows.create())
      .trigger(CountTrigger.of(1))
      .max(1)
      .setParallelism(1)
      .print()
      .setParallelism(1)

    env.execute()
  }

}
