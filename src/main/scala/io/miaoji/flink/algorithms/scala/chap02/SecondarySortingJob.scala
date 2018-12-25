package io.miaoji.flink.algorithms.scala.chap02

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

/**
  * 使用了sortPartition
  */
object SecondarySortingJob {

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
      .map[Array[String]]{x:String => x.split(",")}
      // STEP-2: 过滤不符合格式的参数
      .filter(_.length!=3)
      // STEP-3: 第一个是名称，第二个是日期，第三个是金额
      .map{x:Array[String] => (x(0),x(1), x(2))}
      // STEP-4: 分区然后根据date排序
      // <name>,<date>,<money>
      .partitionByRange(0)
      .sortPartition(2, Order.ASCENDING)
      // STEP-5: 分组然后归约
      // <name>,<date>,<money>
      .groupBy(0)
      .reduce {
        (t1: (String, String, String), t2: (String, String, String)) =>
        (t1._1, t1._2 + ","+t2._2,t1._3+","+t2._3)
      }
      // STEP-6:
      // <name>,<date1,date2>,<money1,money2>
      .flatMap {(t: (String, String, String), coll: Collector[(String, String)]) =>
        val timeArray = t._2.split(",")
        val array = Array(timeArray.length)
        var i = 0
        for( i <- 0 until timeArray.length-1){

          coll.collect((t._1,s"""(${t._2.split(",")(i)},${t._3.split(",")(i)})"""))
        }
      }
      // STEP-7: 名称分组然后把"日期金额"字符串拼接在后面
      // <name>,<date,money>
      .groupBy(0)
      .reduce((t1,t2) => (t1._1,t1._2+t2._2))

    dataSource.print()
    dataSource.writeAsText(OUTPUT_FILE_NAME, WriteMode.OVERWRITE)


    env.execute("top 10")
  }
}
