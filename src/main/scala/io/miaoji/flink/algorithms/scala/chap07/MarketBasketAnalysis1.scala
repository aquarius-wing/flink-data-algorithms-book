package io.miaoji.flink.algorithms.scala.chap07

import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
  * 包: io.miaoji.flink.algorithms.scala.chap07
  * 开发者: wing
  * 开发时间: 2019-02-24
  * 功能：购物篮分析
  * 目的是自动生成关联规则
  *
  */
object MarketBasketAnalysis1 {

  def strSort(strs:(String, String)) :(String, String) ={
    if(strs._1 > strs._2){
      (strs._2,strs._1)
    }else{
      strs
    }
  }

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = TestData.getData(env)
    //region step-1: 商品名称变成相互出现频次
    val itemPair = data.flatMap { (strings: Array[String], collector: Collector[((String, String), Int)]) =>
      for(i <- 0 until strings.length){
        for(j <- i+1 until strings.length){
          val item_pair = strSort((strings(i), strings(j)))
          collector.collect((item_pair, 1))
        }
      }
    }
    //endregion
    //region step-2: 统计出现频次
    itemPair.groupBy(0).sum(1)
      .print()
    //endregion

  }

}
