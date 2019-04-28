package config

import org.apache.hadoop.conf.Configuration


/**
  * Created by Administrator on 2019/4/26.
  * 操作配置文件
  */
object ConfigurationManager {
  private val configuration = new Configuration()
  //加载配置文件
  configuration.addResource("project-config.xml")

  def getvalue(key: String) = {
    configuration.get(key)
  }
}
