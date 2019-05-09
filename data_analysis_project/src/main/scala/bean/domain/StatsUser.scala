package bean.domain

/**
  * Created by Administrator on 2019/5/5.
  */
class StatsUser( val date_dimension_id: Int,
                 val platform_dimension_id: Int,
                 val active_users: Int,
                 val new_install_users: Int,
                 val session_count: Int,
                 val session_length: Int,
                 val created: String) {

}
