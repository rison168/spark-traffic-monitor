package com.rison.traffic.util

import java.io.{InputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * @author : Rison 2021/9/27 上午10:27
 *         读取配置文件工具类
 */
object PropertiesUtils {
  /**
   * 获取配置文件的value
   *
   * @param propertiesName 配置文件路径名称,默认为application.properties
   * @param key            key
   * @return value
   */
  def getValueStr(key: String, propertiesName: String = "application.properties"): String = {
    val properties = new Properties()
    properties.load(new InputStreamReader(
      Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),
      StandardCharsets.UTF_8))
    properties.getProperty(key)
  }
  

}
