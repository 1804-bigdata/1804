package streaming

import Util.Utils
import bean.caseclass.IPRule
import bean.domain.{LocationDimension, StatsLocationFlow}
import bean.domain.dimension.DateDimension
import common.Analysislog
import config.ConfigurationManager
import constants.{GlobalConstants, LogConstants}
import dao.{DimensionDao, StatsLocationFlowDao}
import enum.EventEnum
import jdbc.JdbcHelper
import kafka.KafkaManager
import kafka.serializer.StringDecoder
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2019/5/9.
  */
object FlowTask {

  /**
    * 使用spark加载ip规则库
    */
  def loadIPRule(sc: SparkContext) = {
    val ipRuleArray = sc.textFile(ConfigurationManager.getvalue(GlobalConstants.CONFIG_IP_RULE_DATA_PATH), 2).map(line => {
      val fields = line.split("[|]")
      IPRule(fields(2).toLong, fields(3).toLong, fields(5), fields(6), fields(7))
    }).collect()
    ipRuleArray
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("/checkpoint/A1804")
    //加载ip规则
    val iPRule = loadIPRule(sc)
    //广播ip规则
    val IpRuleBroadcast: Broadcast[Array[IPRule]] = sc.broadcast(iPRule)
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "hadoop1:9092,hadoop2:9092",
      "group.id" -> "A1804",
      "auto.offset.reset" -> "largest"
    )
    val kafkaManager = new KafkaManager(kafkaParams, Set("event_logs"))
    val kafkaInputDstream = kafkaManager.createDirectDstreams[String, String, StringDecoder, StringDecoder, String](ssc)
    kafkaInputDstream.map(logtext => {
      Analysislog.analysisLog(logtext, IpRuleBroadcast.value)
    }).filter(x => x != null).flatMap(m => {
      val accesstime = Utils.formatDate(m(LogConstants.LOG_COLUMNS_NAME_ACCESS_TIME).toLong, "yyyy-MM-dd")
      val country = m(LogConstants.LOG_COLUMNS_NAME_COUNTRY)
      val province = m(LogConstants.LOG_COLUMNS_NAME_PROVINCE)
      val city = m(LogConstants.LOG_COLUMNS_NAME_CITY)
      val eventname = m(LogConstants.LOG_COLUMNS_NAME_EVENT_NAME)
      val uid = m(LogConstants.LOG_COLUMNS_NAME_UID)
      val sid = m(LogConstants.LOG_COLUMNS_NAME_SID)
      Array(
        //全国
        ((accesstime, country, GlobalConstants.VALUE_OF_ALL, GlobalConstants.VALUE_OF_ALL), (eventname, uid, sid)),
        //全省
        ((accesstime, country, province, GlobalConstants.VALUE_OF_ALL), (eventname, uid, sid)),
        //全市
        ((accesstime, country, province, city), (eventname, uid, sid))
      )
    }).updateStateByKey(
      (it: Iterator[((String, String, String, String), Seq[(String, String, String)], Option[Array[Any]])]) => {
        it.map(t3 => {
          //取出当前key
          val key = t3._1
          //取出当前key在之前聚合的结果
          val array = t3._3.getOrElse(Array[Any](0, mutable.Set[String](), 0, mutable.Map[String, Int]()))
          var nu = array(0).asInstanceOf[Int]
          var uidSet = array(1).asInstanceOf[mutable.Set[String]]
          var pv = array(2).asInstanceOf[Int]
          var sidMap = array(3).asInstanceOf[mutable.Map[String, Int]]
          t3._2.foreach(x3 => {
            //x3=(eventName, uid, sid)
            val eventname = x3._1
            val uid = x3._2
            val sid = x3._3
            if (eventname.equals(EventEnum.LAUNCH_EVENT.toString)) nu += 1
            uidSet.add(uid)
            if (eventname.equals(EventEnum.PAGE_VIEW_EVENT.toString) || eventname.equals(EventEnum.BROWSER_PRODUCT_EVENT.toString)) pv += 1
            sidMap.put(sid, sidMap.getOrElse(sid, 0) + 1)
          })
          array(0) = nu
          array(1) = uidSet
          array(2) = pv
          array(3) = sidMap
          (key, array)
        })
      },
      //分区器
      new HashPartitioner(sc.defaultParallelism),
      //是否记住分区
      true
    ).foreachRDD(rdd => {
      //不能再此处创建连接对象，因为此处创建的对象如果在rdd算子中使用，那么就必须进行序列化，而连接对象是不能够被序列化的
      rdd.foreachPartition(partitionIt => {
        val arraybuffer = ArrayBuffer[StatsLocationFlow]()
        val connection = JdbcHelper.getConnection()
        partitionIt.foreach(t2 => {
          val array = t2._2
          val date_dimension_id: Int = DimensionDao.getDimensionId(DateDimension.buildDateDimension(t2._1._1), connection)
          val location_dimension_id: Int = DimensionDao.getDimensionId(new LocationDimension(0, t2._1._2, t2._1._3, t2._1._4), connection)
          val nu = (array(0)).asInstanceOf[Int]
          val uv = (array(1)).asInstanceOf[mutable.Set[String]].size
          val pv = (array(2)).asInstanceOf[Int]
          val sn = (array(3)).asInstanceOf[mutable.Map[String, Int]].size
          val on = (array(3)).asInstanceOf[mutable.Map[String, Int]].filter(x => x._2 == 1).size
          val created = t2._1._1
          arraybuffer.append(new StatsLocationFlow(date_dimension_id, location_dimension_id, nu, uv, pv, sn, on, created))
        })
        if (connection != null)
          connection.close()
        if (arraybuffer.size > 0) {
          StatsLocationFlowDao.updateBatch(arraybuffer.toArray)
        }
      })
      //更新消费偏移量
      kafkaManager.updateConsumerOffsets()
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
