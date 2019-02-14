package io.miaoji.flink.algorithms.scala.chap06


import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

object MovingAverage {

  val INPUT_FILE_NAME = "chap06/input.csv"
  val OUTPUT_FILE_NAME = "chap06/output.txt"
  val dir = MovingAverage.getClass.getClassLoader.getResource("")

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    // STEP-1: 读取数据
    val inputData = env.readCsvFile[(String, String, Double)](dir+"/"+INPUT_FILE_NAME)
    val result = inputData
      .groupBy(0)
      .sortGroup(1, Order.ASCENDING)
      .reduceGroup { (tuples: Iterator[(String, String, Double)], collector: Collector[(String, String, Array[Double])
        ]) =>
        var i = 0
        val array = new ArrayBuffer[Double]
        for (t <- tuples) {
          array.append(t._3)
          if(i >= 2){
            // 当天和前两天
            collector.collect((t._1,t._2,Array(t._3,array(i-1),array(i-2))))
          }else if(i == 1){
            collector.collect((t._1,t._2,Array(t._3,array(i-1))))
          }else{
            collector.collect((t._1,t._2,Array(t._3)))
          }
          i += 1
        }
      }
      .map((tuple: (String, String, Array[Double])) =>
        (tuple._1,tuple._2,tuple._3.sum/tuple._3.length)
      )
    result.writeAsCsv(dir+"/"+OUTPUT_FILE_NAME,writeMode = WriteMode.OVERWRITE).setParallelism(1)
    env.execute()

  }

}
