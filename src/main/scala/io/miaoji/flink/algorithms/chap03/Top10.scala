package io.miaoji.flink.algorithms.chap03

import akka.stream.impl.fusing.Collect
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

object Top10{

  val INPUT_FILE_NAME = "chap03/top10_input.txt"
  val OUTPUT_FILE_NAME = "chap03/top10_output.txt"

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val dir = Top10.getClass.getClassLoader.getResource("")
    val inputFilePath = dir + "/" + INPUT_FILE_NAME
    val outputFilePath = dir + "/" + OUTPUT_FILE_NAME
    val dataSource = env.readTextFile(inputFilePath)
    dataSource
      .map[Array[String]]{x:String => x.split(",")}
      .map{x:Array[String] => (x(0),x(1), x(2))}
      .partitionByRange(0)
      .sortPartition(2, Order.ASCENDING)
      .groupBy(0)
      .reduce {
        (t1: (String, String, String), t2: (String, String, String)) =>
        (t1._1, t1._2 + ","+t2._2,t1._3+","+t2._3)
      }
      .flatMap {(t: (String, String, String), coll: Collector[(String, String)]) =>
        val timeArray = t._2.split(",")
        val array = Array(timeArray.length)
        var i = 0
        for( i <- 0 until timeArray.length-1){

          coll.collect((t._1,s"""(${t._2.split(",")(i)},${t._3.split(",")(i)})"""))
        }
      }
      .groupBy(0)
      .reduce((t1,t2) => (t1._1,t1._2+t2._2))
      .print()


  }
}