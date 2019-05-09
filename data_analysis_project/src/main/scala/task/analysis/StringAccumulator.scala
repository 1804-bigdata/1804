package task.analysis

import Util.Utils
import constants.GlobalConstants

/**
  * Created by Administrator on 2019/4/29.
  */
class StringAccumulator {
  var value: String = null
  init()

  /**
    * 初始化value的值
    * session_count=0|1s_3s=0|4s_6s=0|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0|3m_10m=0|10m_30m=0|30m=0|1_3=0|4_6=0|7_9=0|10_30=0|30_60=0|60=0
    */
  def init() = {
    value = GlobalConstants.SESSION_COUNT + "=" + 0 + "|" +
      GlobalConstants.TIME_1s_3s + "=" + 0 + "|" +
      GlobalConstants.TIME_4s_6s + "=" + 0 + "|" +
      GlobalConstants.TIME_7s_9s + "=" + 0 + "|" +
      GlobalConstants.TIME_10s_30s + "=" + 0 + "|" +
      GlobalConstants.TIME_30s_60s + "=" + 0 + "|" +
      GlobalConstants.TIME_1m_3m + "=" + 0 + "|" +
      GlobalConstants.TIME_3m_10m + "=" + 0 + "|" +
      GlobalConstants.TIME_10m_30m + "=" + 0 + "|" +
      GlobalConstants.TIME_30m + "=" + 0 + "|" +
      GlobalConstants.STEP_1_3 + "=" + 0 + "|" +
      GlobalConstants.STEP_4_6 + "=" + 0 + "|" +
      GlobalConstants.STEP_7_9 + "=" + 0 + "|" +
      GlobalConstants.STEP_10_30 + "=" + 0 + "|" +
      GlobalConstants.STEP_30_60 + "=" + 0 + "|" +
      GlobalConstants.STEP_60 + "=" + 0
  }

  /**
    * 定义一个累加的方法
    */
  def add(fieldName: String) :Unit= {
    //先取出  再累加  后放回去
    synchronized({
      val fieldNewValue = (Utils.getFieldValue(value, fieldName).toInt + 1).toString
      value=Utils.setFieldValue(value, fieldName, fieldNewValue)
    })
  }
}
