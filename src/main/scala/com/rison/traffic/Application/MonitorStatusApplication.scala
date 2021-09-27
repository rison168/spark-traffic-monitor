package com.rison.traffic.Application

import com.rison.traffic.util.SparkUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author : Rison 2021/9/27 上午10:54
 *         1.卡扣状态监控
 *         检测卡扣的状态，其中就是检测卡扣的摄像头是否能正常拍摄
 *
 */
object MonitorStatusApplication {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //配置文件配置是否本地运行
    SparkUtils.setMaster(sparkConf)
    sparkConf.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    val sc = new SparkContext(sparkConf)
    //获取全部卡扣对应的全部全部摄像头数据
    val monitorCameraRDD: RDD[String] = sc.textFile("data/monitor_camera_info")
    //获取当天全部的卡扣车流量数据
    val monitorCarFlowRDD: RDD[String] = sc.textFile("data/monitor_flow_action")

    //卡扣全部摄像头 (mid,(cid1,cid2,...))
    val monitorCameraIdsRDD: RDD[(String, Iterable[String])] = monitorCameraRDD
      .flatMap(_.split("\n"))
      .map(
        line => {
          val Array(mid, cid): Array[String] = line.split("\t")
          (mid, cid)
        }
      )
      .distinct()
      .groupByKey()

    //当天卡扣正常的摄像头 (mid,(cid1,cid2,...))
    val currentMonitorCameraIdsRDD: RDD[(String, Iterable[String])] = monitorCarFlowRDD
      .flatMap(_.split("\n"))
      .map(
        line => {
          val strings: Array[String] = line.split("\t")
          (strings(1), strings(2))
        }
      )
      .distinct()
      .groupByKey()

    //Join 获取到卡扣对应的全部摄像头和当前正常运行的摄像头
    val joinMonitorCameraIdsRDD: RDD[(String, (Iterable[String], Option[Iterable[String]]))] = monitorCameraIdsRDD.leftOuterJoin(currentMonitorCameraIdsRDD)

    //卡扣异常摄像头 (mid, (异常摄像头数目， 异常摄像头id集合))
    val resultRDD: RDD[(String, (Int, List[String]))] = joinMonitorCameraIdsRDD
      .map(
        data => {
          val allCIds: List[String] = data._2._1.toList
          var currCIds: List[String] = List.empty
          data._2._2 match {
            case some: Some[Iterable[_]] => currCIds = some.head.toList
            case None => currCIds = List.empty
          }
          //比较全部摄像头和当前正常摄像头差集
          (data._1, (allCIds.diff(currCIds).size, allCIds.diff(currCIds)))
        }
      )
    resultRDD.collect().foreach(println)
    sc.stop()
  }

}

/**
 * 卡扣异常摄像头 (mid, (异常摄像头数目， 异常摄像头id集合))
 * (0004,(8,List(64257, 78793, 26268, 69020, 92818, 64092, 42685, 84806)))
 * (0006,(7,List(54186, 16753, 82717, 76947, 00738, 89486, 09545)))
 * (0002,(7,List(00912, 46585, 74268, 37234, 02854, 09883, 88136)))
 * (0008,(6,List(32819, 25438, 68285, 92102, 50174, 67575)))
 * (0000,(7,List(29153, 79826, 56956, 42786, 42310, 53581, 61023)))
 * (0001,(8,List(72976, 75882, 17604, 29051, 89297, 03460, 81798, 12537)))
 * (0003,(8,List(01356, 17473, 99230, 31755, 34122, 87412, 77071, 38492)))
 * (0009,(1,List(28104)))
 * (0007,(5,List(38539, 54861, 74057, 68457, 26420)))
 * (0005,(5,List(80049, 03310, 52857, 15793, 30139)))
 */
