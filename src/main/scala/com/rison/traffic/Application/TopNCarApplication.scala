package com.rison.traffic.Application

import com.rison.traffic.constant.Constants
import com.rison.traffic.util.{PropertiesUtils, SparkUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : Rison 2021/9/27 下午4:03
 *         2.通过车辆数最多的TopN卡扣
 */
object TopNCarApplication {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    SparkUtils.setMaster(sparkConf)
    val sc: SparkContext = SparkContext.getOrCreate(config = sparkConf)
    //获取当天全部的卡扣车流量数据
    val monitorCarFlowRDD: RDD[String] = sc.textFile("data/monitor_flow_action")
    //卡扣和车辆集合 (mid, cars)
    val monitorCarsRDD: RDD[(String, Iterable[String])] = monitorCarFlowRDD
      .flatMap(_.split("\n"))
      .map(
        line => {
          val strings: Array[String] = line.split("\t")
          (strings(1), strings(3))
        }
      )
      .groupByKey()
    val monitorCarsTotalRDD: RDD[(String, Int)] = monitorCarsRDD
      .map(
        data => {
          (data._1, data._2.size)
        }
      )
    monitorCarsTotalRDD.sortBy(_._2, false).take(PropertiesUtils.getValueStr(Constants.FIELD_TOP_NUM).toInt).foreach(println)

    sc.stop()
  }
}

/**
 * (卡扣号， 过车数)
 * (0008,7354)
 * (0004,7278)
 * (0000,7252)
 * (0007,7226)
 * (0003,7222)
 */