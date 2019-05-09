package bean.domain

/**
  * Created by Administrator on 2019/5/5.
  */
class StatsDeviceLocation(val date_dimension_id: Int,
                          val platform_dimension_id: Int,
                          val location_dimension_id: Int,
                          val active_users: Int,
                          val session_count: Int,
                          val bounce_sessions: Int,
                          val created: String) {

}
