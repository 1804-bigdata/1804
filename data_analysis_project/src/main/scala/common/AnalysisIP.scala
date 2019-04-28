package common

import Util.Utils
import bean.{IPRule, RegionInfo}

import scala.util.control.Breaks._

/**
  * Created by Administrator on 2019/4/25.
  */
object AnalysisIP {
  /**
    * 通过ip，获取ip对应的地域信息，将地域信息封装到RegionInfo对象中
    * @param ip
    * @param ipRuleArray
    */
  def getRegionInfoByIP(ip: String, ipRuleArray: Array[IPRule]) = {
    val regionInfo = RegionInfo()
    //1.把ip转为十进制数字
    val numip = Utils.ipToLong(ip)
    //2.通过二分查找法，查找相对应ip的地址
    val index = binarySearch(numip, ipRuleArray)
    if (index != -1) {
      //3.将地域信息封装到RegionInfo对象中
      val ipRule = ipRuleArray(index)
      regionInfo.country = ipRule.country
      regionInfo.province = ipRule.province
      regionInfo.city = ipRule.city
    }
    regionInfo
  }

  /**
    * 二分查找法，找到了返回对应的角标，找不到返回-1
    *
    * @param numIP
    * @param ipRuleArray
    */
  def binarySearch(numIP: Long, ipRuleArray: Array[IPRule]) = {
    var index = -1
    var min = 0
    var max = ipRuleArray.length - 1
    breakable({
      while (min <= max) {
        val middle = (min + max) / 2
        val ipRule = ipRuleArray(middle)
        if (numIP >= ipRule.startip && numIP <= ipRule.entip) {
          index = middle
          break()
        } else if (numIP > ipRule.entip) {
          min = middle + 1
        } else if (numIP < ipRule.startip) {
          max = middle - 1
        }
      }
    })
    index
  }
}
