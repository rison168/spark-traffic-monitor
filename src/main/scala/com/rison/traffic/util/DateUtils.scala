package com.rison.traffic.util

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author : Rison 2021/9/28 上午11:39
 *         时间格式工具
 */
object DateUtils {
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

}
