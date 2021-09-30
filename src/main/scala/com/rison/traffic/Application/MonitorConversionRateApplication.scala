package com.rison.traffic.Application

import java.text.NumberFormat

import cn.hutool.core.util.NumberUtil
import com.rison.traffic.util.{DateUtils, SparkUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author : Rison 2021/9/30 上午9:57
 *         8. 卡扣流量转换率
 *         卡扣转换率就是
 *         比如给定一个条件：0001->0002->0003->0004
 *         分别依次求(次数比率)：
 *         (0001->0002->0003->0004， 0001->0002->0003->0004 / 0001->0002->0003)
 *         (0001->0002->0003， 0001->0002->0003 / 0001->0002)
 *         (0001->0002-， 0001->0002 / 0001)
 */
object MonitorConversionRateApplication {
  //给定条件
  val roadFlow = "0001->0002->0003->0004->0005"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    SparkUtils.setMaster(sparkConf)

    val sc: SparkContext = SparkContext.getOrCreate(sparkConf)
    val monitorFlowActionRDD: RDD[String] = sc.textFile("data/monitor_flow_action")

    //这里先要求出所有的 carTrack
    val carTrackRDD: RDD[(String, String)] = monitorFlowActionRDD.flatMap(_.split("\n"))
      .map(
        line => {
          val strings: Array[String] = line.split("\t")
          //(carId, (monitorId, time))
          (strings(3), (strings(1), strings(4)))
        }
      )
      .groupByKey()
      .map(
        data => {
          val list: List[(String, String)] = data._2.toList
          val tuples: List[(String, String)] = list.sortWith(
            (tuple1, tuple2) => {
              DateUtils.getTimeStringToLong(tuple1._2) < DateUtils.getTimeStringToLong(tuple2._2)
            }
          )
          (data._1, tuples.toStream.map(_._1).mkString("->"))
        }
      )

    //广播roadFlow
    val roadFlowBC: Broadcast[String] = sc.broadcast(roadFlow)

    //得到卡扣对应的不同road的count
    val carRoadCountRDD: RDD[(String, List[(String, Int)])] = carTrackRDD
      .map(
        data => {
          //car行车轨迹
          val track: String = data._2
          //定义一个map，记录不同的路线的次数
          val trackMap: mutable.Map[String, Int] = scala.collection.mutable.Map()
          val monitorArr: Array[String] = roadFlowBC.value.split("->")
          var roadTrackTemp: String = ""
          for (i <- 0 until monitorArr.length) {
            roadTrackTemp = roadTrackTemp + monitorArr(i) + "->"
            //道路轨迹
            var roadTrack: String = roadTrackTemp.stripSuffix("->")
            //这里要算出有个多个子集，记录子集个数
            var count = 0
            var index: Int = track.indexOf(roadTrack, 0)
            while (index != -1) {
              count = count + 1
              index = track.indexOf(roadTrack, index + 1)
            }
            trackMap.update(roadTrack, count)
          }
          println((data._1, trackMap.toList))
          (data._1, trackMap.toList)
        }
      )
    //扁平化处理
    val roadCountRDD: RDD[(String, Int)] = carRoadCountRDD.flatMap(data => data._2)
    //聚合计算
    val roadCountReduceByKeyRDD: RDD[(String, Int)] = roadCountRDD.reduceByKey(_ + _)

    val roadCountMap: Map[String, Int] = roadCountReduceByKeyRDD.collect().toMap
    roadCountMap.foreach(println)
    val roadRate: Map[String, String] = rate(roadFlow, roadCountMap)
    println("==========================")
    roadRate.foreach(println)
    sc.stop()
  }

  /**
   * 转换率
   *
   * @param roadTrack    车轨迹条件
   * @param roadCountMap 车轨迹计数
   */
  def rate(roadTrack: String, roadCountMap: Map[String, Int]): Map[String, String] = {
    val tuples = ListBuffer[(String, String)]()
    val trackArr: Array[String] = roadTrack.split("->")
    var track = ""
    var lastCount = 0
    for (i <- 0 until trackArr.length) {
      track = track + trackArr(i) + "->"
      val nextCount: Int = roadCountMap.getOrElse(track.stripSuffix("->"), 0)
      if (lastCount != 0 && i != 0) {
        tuples.append((track.stripSuffix("->"), NumberUtil.formatPercent(nextCount.toDouble / lastCount.toDouble, 2)))
      }
      lastCount = nextCount
    }
    tuples.toMap
  }

}

/**
 * (0001,7166)
 * (0001->0002,759)
 * (0001->0002->0003,80)
 * (0001->0002->0003->0004,5)
 * (0001->0002->0003->0004->0005,0)
 * ==========================
 * (0001->0002,10.59%)
 * (0001->0002->0003,10.54%)
 * (0001->0002->0003->0004,6.25%)
 * (0001->0002->0003->0004->0005,0%)
 */