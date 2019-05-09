package dao

import bean.domain.SessionAggrStat
import jdbc.JdbcHelper

/**
  * Created by Administrator on 2019/4/29.
  */
object SessionAggrStatDao {
  /**
    * 删除结果
    *
    * @param dateDimensionId
    * @return
    */
  def deleteByDateDimensionId(dateDimensionId: Int) = {
    val sql = "delete from session_aggr_stat where date_dimension_id=?"
    val sqlParams = Array[Any](dateDimensionId)
    JdbcHelper.executeUpdate(sql, sqlParams)
  }
  def insertBatch(sessionAggrStatArray: Array[SessionAggrStat]) = {
    val sql = "insert into session_aggr_stat values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    val sqlParamsArray = new Array[Array[Any]](sessionAggrStatArray.length)
    for (i <- 0 until (sessionAggrStatArray.length)) {
      val sessionAggrStat = sessionAggrStatArray(i)
      sqlParamsArray(i) = Array[Any](
        sessionAggrStat.date_dimension_id,
        sessionAggrStat.platform_dimension_id,
        sessionAggrStat.session_count,
        sessionAggrStat.time_1s_3s,
        sessionAggrStat.time_4s_6s,
        sessionAggrStat.time_7s_9s,
        sessionAggrStat.time_10s_30s,
        sessionAggrStat.time_30s_60s,
        sessionAggrStat.time_1m_3m,
        sessionAggrStat.time_3m_10m,
        sessionAggrStat.time_10m_30m,
        sessionAggrStat.time_30m,
        sessionAggrStat.step_1_3,
        sessionAggrStat.step_4_6,
        sessionAggrStat.step_7_9,
        sessionAggrStat.step_10_30,
        sessionAggrStat.step_30_60,
        sessionAggrStat.step_60
      )
    }
    JdbcHelper.executeBatch(sql, sqlParamsArray)
  }
}
