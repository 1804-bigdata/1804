package bean.domain.dimension

import Util.Utils
import enum.DateTypeEnum

/**
  * Created by Administrator on 2019/4/29.
  * 时间维度
  */
class DateDimension(var id: Int, var year: Int, var season: Int, var month: Int, var week: Int, var day: Int,
                    var calendar: String, var dateType: String) {

}
object DateDimension {
  /**
    * 构建日期维度
    *
    * @param inputDate
    * yyyy-MM-dd
    * 2019-04-25
    */
  def buildDateDimension(inputDate: String) = {
    val year: Int = Utils.getDateInfo(inputDate, DateTypeEnum.YEAR)
    val season: Int = Utils.getDateInfo(inputDate, DateTypeEnum.SEASON)
    val month: Int = Utils.getDateInfo(inputDate, DateTypeEnum.MONTH)
    val week: Int = Utils.getDateInfo(inputDate, DateTypeEnum.WEEK)
    val day: Int = Utils.getDateInfo(inputDate, DateTypeEnum.DAY)
    new DateDimension(0, year, season, month, week, day, inputDate, DateTypeEnum.DAY.toString)
  }
}