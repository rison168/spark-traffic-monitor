package com.rison.traffic.util

import com.rison.traffic.constant.Constants
import org.apache.spark.SparkConf

/**
 * @author : Rison 2021/9/27 上午10:25
 *         Spark 工具类
 */
object SparkUtils {
  /**
   * 根据当前是否本地测试配置，决定sparkConf的master
   * @param sparkConf
   */
  def setMaster(sparkConf: SparkConf): Unit = {
    if(PropertiesUtils.getValueStr(Constants.SPARK_LOCAL).toBoolean){
      sparkConf.setMaster("local[*]")
    }
  }
}
