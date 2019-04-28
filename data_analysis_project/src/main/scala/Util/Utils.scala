package Util

import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

/**
  * Created by Administrator on 2019/4/25.
  */
object Utils {
  //将ip转为 十进制数
  def ipToLong(ip: String) = {
    var numip: Long = 0
    val splited = ip.split("[.]")
    for (item <- splited) {
      numip = (numip << 8 | item.toLong)
    }
    numip
  }

  /**
    * 验证日期是否是yyyy-MM-dd这种格式
    *
    * @param inputDate 输入的需要验证的日期 2019-01-01
    */
  def validateInputDate(inputDate: String) = {
    val reg = "^(?:(?!0000)[0-9]{4}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1[0-9]|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[0-9]{2}(?:0[48]|[2468][048]|[13579][26])|(?:0[48]|[2468][048]|[13579][26])00)-02-29)$"
    Pattern.compile(reg).matcher(inputDate).matches()
  }

  /**
    * 将日期转换成时间戳
    *
    * @param inputDate
    * @param pattern
    */
  def parseDate(inputDate: String, pattern: String) = {

    val simpleDateFormat = new SimpleDateFormat(pattern)
    simpleDateFormat.parse(inputDate).getTime
  }

  /**
    * 格式化日期
    *
    * @param longTime 时间戳
    * @param pattern  格式化的格式
    */
  def formatDate(longTime: Long, pattern: String) = {
    val simpleDateFormat = new SimpleDateFormat(pattern)
    simpleDateFormat.format(new Date(longTime))
  }
}
