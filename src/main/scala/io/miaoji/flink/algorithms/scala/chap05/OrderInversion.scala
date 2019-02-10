package io.miaoji.flink.algorithms.scala.chap05


import io.miaoji.flink.algorithms.scala.chap04.LeftOuterJoin
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object OrderInversion {

  val INPUT_FILE_NAME = "chap05/input.txt"
  val OUTPUT_FILE_NAME = "chap05/output.txt"
  val dir = LeftOuterJoin.getClass.getClassLoader.getResource("")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // STEP-1: 读取数据
    val inputData = env.readTextFile(dir+"/"+INPUT_FILE_NAME)
    // java is a great language
    inputData.keyBy(str => str).flatMap(new RichFlatMapFunction[String, (String, Int, Int)] {
      override def flatMap(str: String, collector: Collector[(String, Int, Int)]): Unit = {
        val row = currentRow.value()
        val strArray = str.split(" ")
        for(i <- 0 until strArray.length){
          collector.collect((strArray(i), row, i))
        }
        currentRow.update(row + 1)
      }

      private var currentRow:ValueState[Int] = _

      override def open(parameters: Configuration): Unit = {
        this.currentRow = getRuntimeContext.getState(new ValueStateDescriptor("row", classOf[Int]))
      }
    }).print()
    env.execute()
    // java is
    // java a
    // java great
    // java language
    // is a
    // is great
    // is language
    // a great
    // a language
    // great language

  }

}
