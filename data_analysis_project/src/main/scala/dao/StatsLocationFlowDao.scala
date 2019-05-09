package dao

import bean.domain.StatsLocationFlow
import jdbc.JdbcHelper

/**
  * Created by Administrator on 2019/5/9.
  */
object StatsLocationFlowDao {
  def updateBatch(statsLocationFlowArray: Array[StatsLocationFlow]) = {
    val sql =
      """
        |insert into stats_location_flow(date_dimension_id,location_dimension_id,nu,uv,pv,sn,`on`,created)values(?,?,?,?,?,?,?,?)
        |on duplicate key update nu=?,uv=?,pv=?,sn=?,`on`=?
      """.stripMargin
    val sqlParamsArray = new Array[Array[Any]](statsLocationFlowArray.length)
    for (i <- 0 until (statsLocationFlowArray.length)) {
      val statsLocationFlow = statsLocationFlowArray(i)
      sqlParamsArray(i) = Array[Any](
        statsLocationFlow.date_dimension_id,
        statsLocationFlow.location_dimension_id,
        statsLocationFlow.nu,
        statsLocationFlow.uv,
        statsLocationFlow.pv,
        statsLocationFlow.sn,
        statsLocationFlow.on,
        statsLocationFlow.created,
        statsLocationFlow.nu,
        statsLocationFlow.uv,
        statsLocationFlow.pv,
        statsLocationFlow.sn,
        statsLocationFlow.on
      )
    }
    JdbcHelper.executeBatch(sql, sqlParamsArray)
  }
}
