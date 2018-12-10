package io.miaoji.flink.algorithms

import io.miaoji.flink.algorithms.extra.window.GlobalIntWindows
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{ProcessingTimeTrigger, Trigger}

/**
  * 当前用户 : wing。
  * 当前日期 : 2018/12/9。
  * 包名 : io.miaoji.flink.algorithms.extra。
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
      .flatMap(_.split(" "))
      // 120或者3aw
      .filter((str: String) =>
        str.matches("[0-9]{3}")
      )
      .map((str: String) =>
        ("1",str.toInt)
      )
      .keyBy(0)
      .windowAll(GlobalWindows.create())
      .trigger(ProcessingTimeTrigger.create())
      .max(1)
      .print()

    env.execute()
  }

}
