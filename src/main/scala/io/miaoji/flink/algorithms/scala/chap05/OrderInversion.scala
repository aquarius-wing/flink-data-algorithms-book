package io.miaoji.flink.algorithms.scala.chap05


import io.miaoji.flink.algorithms.scala.chap04.LeftOuterJoin
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

object OrderInversion {

  val INPUT_FILE_NAME = "chap05/input.txt"
  val OUTPUT_FILE_NAME = "chap05/output.txt"
  val dir = OrderInversion.getClass.getClassLoader.getResource("")

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    // STEP-1: 读取数据
    val inputData = env.readTextFile(dir+"/"+INPUT_FILE_NAME)
    // STEP-2: 把句子拆成词与相邻词的元祖
    // java is a great language
    val result = inputData.flatMap { (str: String, collector: Collector[(String, String, Int)]) =>
      val strArray = str.split(" ")
      for(i <- 0 until strArray.length){
        val token = strArray(i)
        val start = if(i - 2 < 0) 0 else i-2
        val end = if(i + 2 >= strArray.length) strArray.length-1 else i+2
        collector.collect((token, "*", end-start))
//        println(s"$i $token $start $end ${end-start}")
        // 这里用to来包括end这个下表
        for(j <- start to end){
          if(j != i) {
            collector.collect((strArray(i), strArray(j), 1))
          }
        }
      }
    }
      // java *         4
      // java is        1
      // java is        1
      // java a         1
      // java great     1
      // java language  1
      // STEP-3: 相同词与相邻词进行相加
      .groupBy(0,1).reduce((t1: (String, String, Int), t2: (String, String, Int)) =>
      (t1._1,t1._2,t1._3 + t2._3)
    )
      // 必须进行sortGroup，才能保证(java,*)是在第一个
      // STEP-4: 按书中的形式组合
      .groupBy(0).sortGroup(2, Order.DESCENDING).reduceGroup { (tuples: Iterator[(String, String, Int)], collector: Collector[(Array[(String, String)], Array[Float])]) =>
      var key = ArrayBuffer[(String, String)]()
      var value = ArrayBuffer[Float]()
      for(tuple <- tuples){
        key.append((tuple._1,tuple._2))
        value.append(tuple._3)
      }
      collector.collect((key.toArray, value.toArray))
    }
      // (java,*),(java,is),(java,a),(java,great),(java,language) 4,1,1,1,1
      .map { (tuple: (Array[(String, String)], Array[Float])) =>
      val key = tuple._1
      val value = tuple._2
      val target = ArrayBuffer[Float]()
      var index = 0
      value.foreach(x => target.append(x/value(index).toFloat))
      (tuple._1, target.toArray)
    }
      // (java,*),(java,is),(java,a),(java,great),(java,language) 1,0.25,0.25,0.25,0.25
      .flatMap ((tuple: (Array[(String, String)], Array[Float]), collector: Collector[(String, String, Double)]) =>
      for (i <- 0 until tuple._1.length) {
        collector.collect(tuple._1(i)._1, tuple._1(i)._2, tuple._2(i))
      }
    )
      .filter(_._2 != "*")
    result.writeAsCsv(dir+"/"+OUTPUT_FILE_NAME,writeMode = WriteMode.OVERWRITE).setParallelism(1)
    env.execute("Order Inversion")
  }

}
