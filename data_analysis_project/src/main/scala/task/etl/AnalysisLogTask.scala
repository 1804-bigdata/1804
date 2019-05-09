package task.etl

import Util.Utils
import bean.caseclass.IPRule
import common.Analysislog
import config.ConfigurationManager
import constants.{GlobalConstants, LogConstants}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import task.BaseTask

import scala.collection.mutable

/**
  * Created by Administrator on 2019/4/25.
  */
object AnalysisLogTask extends BaseTask {

  var inputPath: String = null


  /**
    *2.验证当天是否存在日志信息
    * /logs/2019/04/26/xxx.log
    * 2019-04-26===Long类型时间戳===>2019/04/26==>/logs/2019/04/26==>验证这个路径在hdfs上是否存在
    */
  def validateExistsLog() = {
    inputPath = ConfigurationManager.getvalue(GlobalConstants.CONFIG_LOG_PATH_PREFIX) + Utils.formatDate(Utils.parseDate(inputDate, "yyyy-MM-dd"), "yyyy/MM/dd")
    var fileSystem: FileSystem = null
    try {
      fileSystem = FileSystem.newInstance(configuration)
      if (!fileSystem.exists(new Path(inputPath))) {
        throw new SparkException(
          s"""
             |Usage:etl.AnalysisLogTask
             |errorMessage:指定的日期${inputDate},不存在需要解析的用户行为日志
         """.stripMargin
        )
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (fileSystem != null)
        fileSystem.close()
    }
  }

  /**
    * 使用spark加载ip规则库
    */
  def loadIPRule() = {
    val ipRuleArray = sc.textFile(ConfigurationManager.getvalue(GlobalConstants.CONFIG_IP_RULE_DATA_PATH), 2).map(line => {
      val fields = line.split("[|]")
      IPRule(fields(2).toLong, fields(3).toLong, fields(5), fields(6), fields(7))
    }).collect()
    ipRuleArray
  }

  /**
    * 对用户的行为日志进行解析
    *
    * @param ipRuleArray
    */
  def loadLogFromHdfs(ipRuleArray: Array[IPRule]) = {
    /**
      * 广播变量是共享变量，每个task都能共享，这样不至于每个task都需要去driver端拷贝这个副本
      * 可以降低网络流量消耗，降低executor内存消耗，加快spark作用运行效率，降低失败概率
      */
    val iPRulesBroadcast = sc.broadcast(ipRuleArray)
    val logrdd = sc.textFile(inputPath, 2).map(logtext => {
      Analysislog.analysisLog(logtext, iPRulesBroadcast.value)
    }).filter(x =>
      if (x != null) {
        true
      } else {
        false
      })
    logrdd
  }

  def saveLogToHbase(logrdd: RDD[mutable.Map[String, String]]) = {
    val jobConf = new JobConf(configuration)
    //指定那个类的数据写入到Hbase
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, LogConstants.HBASE_LOG_TABLE_NAME)
    logrdd.map(map => {
      //map==>put
      //构建rowKey 唯一，散列，不能过长，满足业务查询需要 accessTime+"_"+(uid+eventName).hascode
      val accesstime = map(LogConstants.LOG_COLUMNS_NAME_ACCESS_TIME)
      val uid = map(LogConstants.LOG_COLUMNS_NAME_UID)
      val eventname = map(LogConstants.LOG_COLUMNS_NAME_EVENT_NAME)
      val rowkey = accesstime + "_" + Math.abs((uid + eventname).hashCode)
      val put = new Put(rowkey.getBytes())
      map.foreach(t2 => {
        val key = t2._1
        val value = t2._2
        put.addColumn(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes(), key.getBytes(), value.getBytes())
      })
      (new ImmutableBytesWritable(), put)
    }).saveAsHadoopDataset(jobConf)
  }

  def main(args: Array[String]): Unit = {
    // 1,验证参数是否正确
    validateInputArgs(args)
    // 2,验证当天是否存在用户行为日志
    validateExistsLog()
    // 3,使用spark加载ip规则
    val ipRuleArray = loadIPRule()
    // 4,使用spark加载用户行为日志，进行解析
    val logrdd = loadLogFromHdfs(ipRuleArray)
    logrdd.cache()
    // 5,将解析好的日志，保存到hbase上
    saveLogToHbase(logrdd)
    sc.stop()
  }
}
