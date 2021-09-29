package com.rison.traffic.util

import java.text.SimpleDateFormat
import java.util.Date

import cn.hutool.core.date.DateUtil

/**
 * @author : Rison 2021/9/28 上午11:39
 *         时间格式工具
 */
object DateUtils extends DateUtil{
  /**
   * 时间字符串转Long
   *
   * @param time
   * @return
   */
  def getTimeStringToLong(time: String): Long = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date: Date = format.parse(time)
    date.getTime
  }

  /**
   * 时间字符串转Date
   * @param time
   * @return
   */
  def getTimeStringToDate(time: String): Date = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date: Date = format.parse(time)
    date
  }


}
