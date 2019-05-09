package constants

/**
  * Created by Administrator on 2019/4/25.
  */
object LogConstants {
  //存放日志的表名
  val HBASE_LOG_TABLE_NAME = "ns1:event_log"
  //日志表的列族名称
  val HBASE_LOG_TABLE_FAMILY = "log"

  val LOG_COLUMNS_NAME_IP = "ip"
  val LOG_COLUMNS_NAME_ACCESS_TIME = "access_time"
  val LOG_COLUMNS_NAME_COUNTRY = "country"
  val LOG_COLUMNS_NAME_PROVINCE = "province"
  val LOG_COLUMNS_NAME_CITY = "city"
  val LOG_COLUMNS_NAME_UID = "uid"
  val LOG_COLUMNS_NAME_SID = "sid"
  val LOG_COLUMNS_NAME_EVENT_NAME = "en"
  val LOG_COLUMNS_NAME_PLATFORM = "pl"
  val LOG_COLUMNS_NAME_BROWSER_NAME = "b_n"
  val LOG_COLUMNS_NAME_PRODUCT_ID = "pid"
  val LOG_COLUMNS_NAME_OS_NAME = "os_n"
}
