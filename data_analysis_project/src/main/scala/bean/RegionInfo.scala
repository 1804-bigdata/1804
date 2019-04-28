package bean

import constants.GlobalConstants

/**
  * Created by Administrator on 2019/4/25.
  */
case class RegionInfo(var country:String=GlobalConstants.DEFAULT_VALUE, var province:String=GlobalConstants.DEFAULT_VALUE, var city:String=GlobalConstants.DEFAULT_VALUE)
