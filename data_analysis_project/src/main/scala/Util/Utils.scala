package Util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import java.util.regex.Pattern

import enum.DateTypeEnum

import scala.math.BigDecimal.RoundingMode
import scala.util.control.Breaks._
import java.math.BigDecimal

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

  /**
    * 获取指定的下一天时间
    *
    * @param longTime
    */
  def getNextDate(longTime: Long): Long = {
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(longTime)
    calendar.add(Calendar.DAY_OF_MONTH, 1)
    calendar.getTimeInMillis
  }

  /**
    * 获取指定字符串的值
    *
    * @param value
    * session_count=0|1s_3s=0|4s_6s=0|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0|3m_10m=0|10m_30m=0|30m=0|1_3=0|4_6=0|7_9=0|10_30=0|30_60=0|60=0
    * @param fieldName
    * session_count
    */
  def getFieldValue(value: String, fieldName: String) = {
    var fieldValue: String = null
    val items = value.split("[|]")
    breakable({
      for (itme <- items) {
        val kv = itme.split("[=]")
        if (kv(0).equals(fieldName)) {
          fieldValue = kv(1)
          break()
        }
      }
    })
    fieldValue
  }

  /**
    * 设置字符串中指定字段的值
    *
    * @param value
    * session_count=0|1s_3s=0|4s_6s=0|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0|3m_10m=0|10m_30m=0|30m=0|1_3=0|4_6=0|7_9=0|10_30=0|30_60=0|60=0
    * @param fieldName
    * session_count
    * @param fieldNewValue
    * 100
    * @return
    * session_count=100|1s_3s=0|4s_6s=0|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0|3m_10m=0|10m_30m=0|30m=0|1_3=0|4_6=0|7_9=0|10_30=0|30_60=0|60=0
    */
  def setFieldValue(value: String, fieldName: String, fieldNewValue: String) = {
    val items = value.split("[|]")
    breakable({
      for (i <- 0 until (items.length)) {
        val item = items(i)
        val kv = item.split("[=]")
        if (kv(0).equals(fieldName)) {
          items(i) = fieldName + "=" + fieldNewValue
          break()
        }
      }
    })
    items.mkString("|")
  }

  /**
    * 四舍五入函数
    *
    * @param doubleValue
    * 需要进行四舍五入的数据
    * @param scale
    * 保留的小数位
    */
  def getScale(doubleValue: Double, scale: Int) = {
    val bigDecimal = new BigDecimal(doubleValue)
    bigDecimal.setScale(scale, RoundingMode.HALF_UP).doubleValue()
  }

  /**
    * 获取单位信息 年 季节 月 周 天
    *
    * @param inputDate
    * @param dateType
    */
  def getDateInfo(inputDate: String, dateType: DateTypeEnum.Value) = {
    val longTime = parseDate(inputDate, "yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(longTime)
    if (dateType.equals(DateTypeEnum.YEAR)) {
      calendar.get(Calendar.YEAR)
    } else if (dateType.equals(DateTypeEnum.SEASON)) {
      //获取的到月份是0-11月所以要加1
      val month = calendar.get(Calendar.MONTH) + 1
      if (month % 3 == 0) {
        month / 3
      } else {
        month / 3 + 1
      }
    } else if (dateType.equals(DateTypeEnum.MONTH)) {
      calendar.get(Calendar.MONTH) + 1
    } else if (dateType.equals(DateTypeEnum.WEEK)) {
      calendar.get(Calendar.WEEK_OF_YEAR)
    } else {
      calendar.get(Calendar.DAY_OF_MONTH)
    }
  }
}
