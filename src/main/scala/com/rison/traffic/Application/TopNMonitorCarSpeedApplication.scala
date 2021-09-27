package com.rison.traffic.Application

import com.rison.traffic.util.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * @author : Rison 2021/9/27 下午5:22
 *         4. 车辆通过速度相对比较快的TopN卡扣
 *         车速：
 *         120 =< speed 		   高速
 *         90 <= speed < 120	 中速
 *         60 <= speed < 90	   正常
 *         0 < speed < 60		   低速
 *         按照 高速车数->中速->正常->低速 依次排序
 */
object TopNMonitorCarSpeedApplication {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    SparkUtils.setMaster(sparkConf)
    val sc: SparkContext = SparkContext.getOrCreate(config = sparkConf)
    //获取当天全部的卡扣车流量数据
    val monitorCarFlowRDD: RDD[String] = sc.textFile("data/monitor_flow_action")
    val accumulator: CarSpeedLevelCountAccumulator = CarSpeedLevelCountAccumulator()
    sc.register(accumulator, "carSpeedLevelCountAccumulator")
    monitorCarFlowRDD
      .flatMap(_.split("\n"))
      .map(
        data => {
          val strings: Array[String] = data.split("\t")
          (strings(1), strings(5).toDouble)
        }
      )
      .foreach(
        data => {
          accumulator.add(data)
        }
      )
    accumulator
      .value
      .map(
        data => (SpeedLevel(data._2._1, data._2._2, data._2._3, data._2._4), data._1)
      )
      .toList.sortBy(_._1)
      .foreach(println)
    sc.stop()
  }
}

/**
 * 定义车辆速度等级计数累加器
 */
case class CarSpeedLevelCountAccumulator() extends AccumulatorV2[(String, Double), scala.collection.mutable.Map[String, (Int, Int, Int, Int)]] {
  var map: mutable.Map[String, (Int, Int, Int, Int)] = mutable.Map[String, (Int, Int, Int, Int)]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[(String, Double), mutable.Map[String, (Int, Int, Int, Int)]] = CarSpeedLevelCountAccumulator()

  override def reset(): Unit = map.clear()

  override def add(car: (String, Double)): Unit = {
    val (high: Int, medium: Int, normal: Int, low: Int) = map.getOrElse(car._1, (0, 0, 0, 0))
    //高速
    if (car._2 >= 120) {
      map.update(car._1, (high + 1, medium, normal, low))
    }
    //中速
    else if (car._2 >= 90 && car._2 < 120) {
      map.update(car._1, (high, medium + 1, normal, low))
    }
    //正常
    else if (car._2 >= 60 && car._2 < 90) {
      map.update(car._1, (high, medium, normal + 1, low))
    }
    //低速
    else if (car._2 < 60) {
      map.update(car._1, (high, medium, normal, low + 1))
    }
  }

  override def merge(other: AccumulatorV2[(String, Double), mutable.Map[String, (Int, Int, Int, Int)]]): Unit = {
    other.value.foreach(
      data => {
        var (high: Int, medium: Int, normal: Int, low: Int) = map.getOrElse(data._1, (0, 0, 0, 0))
        map.update(data._1, (high + data._2._1, medium + data._2._2, normal + data._2._3, low + data._2._4))
      }
    )
  }

  override def value: mutable.Map[String, (Int, Int, Int, Int)] = map
}

/**
 * 速度等级依次排序
 * @param high
 * @param medium
 * @param normal
 * @param low
 */
case class SpeedLevel(high: Int, medium: Int, normal: Int, low: Int) extends Ordered[SpeedLevel] with Serializable {
  override def compare(that: SpeedLevel): Int = {
    if (high - that.high != 0) {
      that.high - high
    } else if (medium - that.medium != 0) {
      that.medium - medium
    } else if (normal - that.normal != 0) {
      that.normal - normal
    }else{
      that.low - low
    }
  }
}