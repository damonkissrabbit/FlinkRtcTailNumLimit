package com.damon.utils

import java.util.regex.Pattern

object RegularExpressUtil {

  // 判断当前传入的号牌能否被正确识别
  def isValid(str: String): Boolean = {
    //民用（包括新能源）
    val pattern1 = Pattern.compile("^[\u4e00-\u9fa5]{1}[A-Z]{1}[A-Z0-9]{5,6}$")
    //警察
    val pattern2 = Pattern.compile("^[\u4e00-\u9fa5]{1}[A-Z]{1}[A-Z0-9]{4}[\\u4e00-\\u9fa5]{1}$")
    //武警
    val pattern3 = Pattern.compile("^WJ[\u4e00-\u9fa5]{0,1}[A-Z0-9]{5}$")
    //军队
    val pattern4 = Pattern.compile("^[A-Z]{1}[A-Z]{1}[0-9]{5}$")

    val matcher1 = pattern1.matcher(str)
    val matcher2 = pattern2.matcher(str)
    val matcher3 = pattern3.matcher(str)
    val matcher4 = pattern4.matcher(str)
    if (matcher1.matches || matcher2.matches || matcher3.matches || matcher4.matches) true
    else false
  }

}
