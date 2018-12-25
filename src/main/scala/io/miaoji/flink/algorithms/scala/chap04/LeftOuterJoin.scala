package io.miaoji.flink.algorithms.scala.chap04

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * 当前用户 : wing。
  * 当前日期 : 2018/12/25。
  * 包名 : io.miaoji.flink.algorithms.scala.chap04。
  * 当前时间 : 4:57 PM。
  * 功能 :
  */
object LeftOuterJoin {

  val USER_INPUT_FILE_NAME = "chap04/user_input.txt"
  val TRANSACTION_INPUT_FILE_NAME = "chap04/transaction_input.txt"
  val OUTPUT_FILE_NAME = "chap04/left_out_join_output.txt"
  val dir = LeftOuterJoin.getClass.getClassLoader.getResource("")

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    // STEP-1: 读取用户数据
    val userData = env.readCsvFile[(String, String)](dir+"/"+USER_INPUT_FILE_NAME)
    // STEP-2: 读取交易数据
    val transactionData = env.readCsvFile[(String, String, String, Long, Long)](dir+"/"+TRANSACTION_INPUT_FILE_NAME)
    // STEP-3: 左联接
    val result = transactionData.leftOuterJoin(userData)
      .where(2).equalTo(0)
        .apply((t: (String, String, String, Long, Long), u: (String, String)) =>
          (t._2,u._2)
        )
    // STEP-4: 合并
      // p1 TX
      .distinct(0,1)
      .groupBy(0).reduce { (t1: (String, String), t2: (String, String)) =>
        (t1._1, t1._2 + "," + t2._2)
    }
      // p1 TX,TX
      .map((t: (String, String)) =>
      (t._1, "["+t._2+"]",t._2.split(",").length)
    )
    result.writeAsCsv(dir+"/"+OUTPUT_FILE_NAME,writeMode = WriteMode.OVERWRITE).setParallelism(1)
    // STEP-4: 输出数据
    env.execute("Left Outer Join")
  }

}
