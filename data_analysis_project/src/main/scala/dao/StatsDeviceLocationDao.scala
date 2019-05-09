package dao

import bean.domain.StatsDeviceLocation
import jdbc.JdbcHelper

/**
  * Created by Administrator on 2019/5/5.
  */
object StatsDeviceLocationDao {
  def deleteByDateDimensionId(dateDimensionId: Int) = {
    val sql = "delete from stats_device_location where date_dimension_id=?"
    val sqlParams = Array[Any](dateDimensionId)
    JdbcHelper.executeUpdate(sql, sqlParams)
  }

  def insertBatch(statsDeviceLocationArray: Array[StatsDeviceLocation]) = {
    val sql = "insert into stats_device_location values(?,?,?,?,?,?,?)"
    val sqlParamsArray = new Array[Array[Any]](statsDeviceLocationArray.length)
    for (i <- 0 until (statsDeviceLocationArray.length)) {
      val statsDeviceLocation = statsDeviceLocationArray(i)
      sqlParamsArray(i) = Array[Any](
        statsDeviceLocation.date_dimension_id,
        statsDeviceLocation.platform_dimension_id,
        statsDeviceLocation.location_dimension_id,
        statsDeviceLocation.active_users,
        statsDeviceLocation.session_count,
        statsDeviceLocation.bounce_sessions,
        statsDeviceLocation.created
      )
    }
    JdbcHelper.executeBatch(sql, sqlParamsArray)
  }
}
