package com.rison.traffic.Application

import com.rison.traffic.util.SparkUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author : Rison 2021/9/30 下午4:19
 *         9.实时道路拥堵情况
 */
object RoadRealTimeApplication {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    SparkUtils.setMaster(sparkConf)

    val ssc = new StreamingContext(sparkConf, Seconds(5))
//    ssc.checkpoint("cp")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val value: DStream[(String, (Int, Int))] = lines
      .map(
        data => {
          val strings: Array[String] = data.split(" ")
          (strings(0), (1.toInt, strings(2).toInt))
        }
      )
      .reduceByKeyAndWindow((v1: (Int, Int), v2: (Int, Int)) => (v1._1 + v2._1, v1._2 + v2._2), Seconds(10), Seconds(2))
    value.print()


    ssc.start()
    ssc.awaitTermination()


  }

}
