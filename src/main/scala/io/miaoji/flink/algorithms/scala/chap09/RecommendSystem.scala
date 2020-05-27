package io.miaoji.flink.algorithms.scala.chap09

import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
 * 包: io.miaoji.flink.algorithms.scala.chap08
 * 开发者: wing
 * 开发时间: 2019-05-25
 * 功能：共同好友
 *
 */
object RecommendSystem {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    TestData.getData(env)
    // "u1", "i1,i3,i6,i7,i10"
    // "u2", "i1,i6"
      .flatMap[(String, String)]((t: (String, String), c: Collector[(String, String)]) => {
        for(itemId <- t._2.split(",")){
          c.collect((itemId, t._1))
        }
      })
    // "i1", "u1"
    // "i1", "u2"
      .leftOuterJoin(TestData.getData(env))
      .where(1).equalTo(0).apply[(String, String)](
      (t1: (String, String), t2: (String, String)) => {
        var itemIds = ""
        for(itemId <- t2._2.split(",")){
          if(itemId != t1._1){
            itemIds = itemIds + itemId + ","
          }
        }
        itemIds = itemIds.substring(0, itemIds.length - 1)
        (t1._1, itemIds)
      }
    )

    // "i1", "i3,i6,i7,i10"
    // "i1", "i6"

    // "i1", "(i3,1),(i6,2),(i7,1),(i10,1)"

    // "i1", "(i6,2),(i6,1),(i7,1),(i10,1)"


  }

}
