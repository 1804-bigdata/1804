package task.analysis

import Util.Utils
import bean.domain._
import bean.domain.dimension.{DateDimension, PlatformDimension}
import constants.{GlobalConstants, LogConstants}
import dao._
import enum.EventEnum
import jdbc.JdbcHelper
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import task.BaseTask

import scala.collection.mutable

/**
  * Created by Administrator on 2019/4/28.
  */
object SessionAnalysisTask extends BaseTask {
  /**
    * 从hbase中加载指定日期当前的所有日志
    */
  def loadDataFromHbase(inputDate: String) = {
    val startTime = Utils.parseDate(inputDate, "yyyy-MM-dd").toString
    val endTime = Utils.getNextDate(startTime.toLong).toString
    val scan = new Scan()
    //设置开启扫描的位置
    scan.setStartRow(startTime.getBytes())
    //设置结束扫描的位置
    scan.setStopRow(endTime.getBytes())
    //scan==>string的scan （base64）
    val protoscan = ProtobufUtil.toScan(scan)
    //使用base64算法对protoscan进行编码，编码成字符串
    val base64sScan = Base64.encodeBytes(protoscan.toByteArray)
    val jobConf = new JobConf(configuration)
    //设置需要加载的表
    jobConf.set(TableInputFormat.INPUT_TABLE, LogConstants.HBASE_LOG_TABLE_NAME)
    //设置扫描器
    jobConf.set(TableInputFormat.SCAN, base64sScan)
    val ResultRdd: RDD[Result] = sc.newAPIHadoopRDD(jobConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(_._2)
    val eventLogRDD = ResultRdd.map(result => {
      val uid = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_UID.getBytes()))
      val sid = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_SID.getBytes()))
      val accessTime = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_ACCESS_TIME.getBytes()))
      val eventName = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_EVENT_NAME.getBytes()))
      val country = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_COUNTRY.getBytes()))
      val province = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_PROVINCE.getBytes()))
      val city = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_CITY.getBytes()))
      val platform = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_PLATFORM.getBytes()))
      val browserName = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_BROWSER_NAME.getBytes()))
      val productId = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_PRODUCT_ID.getBytes()))
      val osName = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_OS_NAME.getBytes()))
      ((uid, sid, accessTime, eventName, country, province, city, platform, browserName, productId, osName))
    })
    eventLogRDD
  }

  /**
    * 按时间和平台维度对我们的session进行统计分析，将结果保持到mysql表中
    *
    * @param eventLogRDD
    * (uid, sid, accessTime, eventName, country, province, city, platform, browserName, productId, osName)
    **/
  def sessionVisitTimeAndStepLengthAnalysisStat(eventLogRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String)]) = {
    //取出需要的字段
    //t4Rdd==>( sid, accessTime, eventName,platform)
    val t4Rdd = eventLogRDD.map(t11 => (t11._2, t11._3, t11._4, t11._8))
    /**
      * 以时间accessTime和platform平台作为key，
      * 以sid, accessTime, eventName,platform作为value((accessTime,platform),( sid, accessTime, eventName,platform))
      * ((2019-04-25,pc),(7E900297-0686-4BA1-8048-90243279B696,1556199190643,e_pv,pc))--数据类型
      * ((2019-04-25,ios),(E2ABF97F-4B3E-49AA-9ABF-6C42EA232C63,1556199191517,e_pv,ios))--数据类型
      */
    val t2Rdd = t4Rdd.map(t4 => ((Utils.formatDate(t4._2.toLong, "yyyy-MM-dd"), t4._4), t4))
    //根据平台维度，一条日志需要变成2条日志
    val flatmapRdd = t2Rdd.flatMap(t2 => {
      Array(
        //所有平台
        ((t2._1._1, GlobalConstants.VALUE_OF_ALL), t2._2),
        //具体平台
        t2
      )
    })
    /**
      * 将同一天同一个平台的数据聚合在一起
      * ((accessTime,platform),List(( sid, accessTime, eventName,platform),( sid, accessTime, eventName,platform),...))
      */
    val groupByKeyRdd = flatmapRdd.groupByKey()
    /**
      * 计算出同一天同一个平台中，每个session的访问时长和访问步长
      * ((accessTime,platform),List((sid,visitTimeLength,visitStepLength),(sid,visitTimeLength,visitStepLength),..))
      */
    val sessionTimeAndStepLengthRDD = groupByKeyRdd.map(t2 => {
      /**
        * t2==> ((accessTime,platform),List(( sid, accessTime, eventName,platform),( sid, accessTime, eventName,platform),...))
        */
      val it = t2._2.groupBy(_._1).map(g => {
        /**
          * 按会话id进行分组 g==>(sid,List(( sid, accessTime, eventName,platform),( sid, accessTime, eventName,platform)))
          */
        var visitStepLength, visitTimeLength: Long = 0L
        var startTime, endTime: Long = 0L
        g._2.foreach(t4 => {
          val eventname = t4._3
          if (eventname.equals(EventEnum.PAGE_VIEW_EVENT.toString) ||
            eventname.equals(EventEnum.BROWSER_PRODUCT_EVENT.toString)) {
            visitStepLength += 1
          }
          val accessTime = t4._2.toLong
          if (startTime == 0 || accessTime < startTime) startTime = accessTime
          if (endTime == 0 || endTime < accessTime) endTime = accessTime
        })
        visitTimeLength = (endTime - startTime) / 1000
        (g._1, visitTimeLength, visitStepLength)
      })
      //((accessTime,platform),List((sid,visitTimeLength,visitStepLength),(sid,visitTimeLength,visitStepLength),..)) 数据类型
      (t2._1, it)
    })
    /**
      * 判断同一天同一个平台，每个session访问时长和步长所属区间，对应区间+1
      *
      * "session_count=0|1s_3s=1|4s_6s=0|....."
      */
    val sessionTimeAndStepLengthRangeRDD = sessionTimeAndStepLengthRDD.map(t2 => {
      val stringAccumulator = new StringAccumulator()
      t2._2.foreach(t3 => {
        stringAccumulator.add(GlobalConstants.SESSION_COUNT)
        val visitTimeLength = t3._2
        val visitStepLength = t3._3
        if (visitTimeLength >= 0 && visitTimeLength <= 3) {
          stringAccumulator.add(GlobalConstants.TIME_1s_3s)
        } else if (visitTimeLength >= 4 && visitTimeLength <= 6) {
          stringAccumulator.add(GlobalConstants.TIME_4s_6s)
        } else if (visitTimeLength >= 7 && visitTimeLength <= 9) {
          stringAccumulator.add(GlobalConstants.TIME_7s_9s)
        } else if (visitTimeLength >= 10 && visitTimeLength <= 30) {
          stringAccumulator.add(GlobalConstants.TIME_10s_30s)
        } else if (visitTimeLength > 30 && visitTimeLength <= 60) {
          stringAccumulator.add(GlobalConstants.TIME_30s_60s)
        } else if (visitTimeLength > 1 * 60 && visitTimeLength <= 3 * 60) {
          stringAccumulator.add(GlobalConstants.TIME_1m_3m)
        } else if (visitTimeLength > 3 * 60 && visitTimeLength <= 10 * 60) {
          stringAccumulator.add(GlobalConstants.TIME_3m_10m)
        } else if (visitTimeLength > 10 * 60 && visitTimeLength <= 30 * 60) {
          stringAccumulator.add(GlobalConstants.TIME_10m_30m)
        } else if (visitTimeLength > 30 * 60) {
          stringAccumulator.add(GlobalConstants.TIME_30m)
        }
        if (visitStepLength >= 1 && visitStepLength <= 3) {
          stringAccumulator.add(GlobalConstants.STEP_1_3)
        } else if (visitStepLength >= 4 && visitStepLength <= 6) {
          stringAccumulator.add(GlobalConstants.STEP_4_6)
        } else if (visitStepLength >= 7 && visitStepLength <= 9) {
          stringAccumulator.add(GlobalConstants.STEP_7_9)
        } else if (visitStepLength >= 10 && visitStepLength <= 30) {
          stringAccumulator.add(GlobalConstants.STEP_10_30)
        } else if (visitStepLength > 30 && visitStepLength <= 60) {
          stringAccumulator.add(GlobalConstants.STEP_30_60)
        } else if (visitStepLength > 60) {
          stringAccumulator.add(GlobalConstants.STEP_60)
        }
      })
      (t2._1, stringAccumulator.value)
    })
    /**
      * 数据结果
      * ((2019-04-25,ios),session_count=646|1s_3s=555|4s_6s=1|7s_9s=0|10s_30s=8|30s_60s=11|1m_3m=25|3m_10m=46|10m_30m=0|30m=0|1_3=646|4_6=0|7_9=0|10_30=0|30_60=0|60=0)
      * ((2019-04-25,pc),session_count=1025|1s_3s=577|4s_6s=1|7s_9s=3|10s_30s=7|30s_60s=23|1m_3m=83|3m_10m=331|10m_30m=0|30m=0|1_3=890|4_6=72|7_9=29|10_30=33|30_60=1|60=0)
      * ((2019-04-25,all),session_count=1713|1s_3s=1134|4s_6s=2|7s_9s=5|10s_30s=15|30s_60s=37|1m_3m=116|3m_10m=404|10m_30m=0|30m=0|1_3=1576|4_6=74|7_9=29|10_30=33|30_60=1|60=0)
      * ((2019-04-25,android),session_count=42|1s_3s=2|4s_6s=0|7s_9s=2|10s_30s=0|30s_60s=3|1m_3m=8|3m_10m=27|10m_30m=0|30m=0|1_3=40|4_6=2|7_9=0|10_30=0|30_60=0|60=0)
      *
      * SessionAggrStat
      * SessionAggrStat
      * SessionAggrStat
      * SessionAggrStat
      **/
    val connection = JdbcHelper.getConnection()
    val sessionAggrStatArray = sessionTimeAndStepLengthRangeRDD.collect().map(t2 => {
      val sessionAggrStat = new SessionAggrStat()
      val accessTime = t2._1._1
      val platform = t2._1._2
      sessionAggrStat.date_dimension_id = DimensionDao.getDimensionId(DateDimension.buildDateDimension(accessTime), connection)
      sessionAggrStat.platform_dimension_id = DimensionDao.getDimensionId(new PlatformDimension(0, platform), connection)
      sessionAggrStat.session_count = Utils.getFieldValue(t2._2, GlobalConstants.SESSION_COUNT).toInt
      sessionAggrStat.time_1s_3s = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.TIME_1s_3s).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.time_4s_6s = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.TIME_4s_6s).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.time_7s_9s = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.TIME_7s_9s).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.time_10s_30s = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.TIME_10s_30s).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.time_30s_60s = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.TIME_30s_60s).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.time_1m_3m = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.TIME_1m_3m).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.time_3m_10m = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.TIME_3m_10m).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.time_10m_30m = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.TIME_10m_30m).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.time_30m = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.TIME_30m).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.step_1_3 = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.STEP_1_3).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.step_4_6 = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.STEP_4_6).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.step_7_9 = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.STEP_7_9).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.step_10_30 = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.STEP_10_30).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.step_30_60 = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.STEP_30_60).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.step_60 = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.STEP_60).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat
    })
    if (connection != null) connection.close()
    //将sessionAggrStatArray保持到mysql中
    SessionAggrStatDao.deleteByDateDimensionId(sessionAggrStatArray(0).date_dimension_id)
    SessionAggrStatDao.insertBatch(sessionAggrStatArray)
  }

  /**
    * 统计同一天同一个平台活跃用户数，新增用户数，会话个数
    *
    * @param eventLogRDD
    * (uid, sid, accessTime, eventName, country, province, city, platform, browserName, productId, osName)
    */
  def userStats(eventLogRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String)]) = {
    val tuple11Rdd = eventLogRDD.map(t11 => ((t11._1, t11._2, t11._3, t11._4, t11._8)))
    // tuple2Rdd=((accessTime,platform),(uid, sid,eventName ))
    val tuple2Rdd = tuple11Rdd.map(t5 => ((Utils.formatDate(t5._3.toLong, "yyyy-MM-dd"), t5._5), (t5._1, t5._2, t5._4)))
    val flatMapRDD = tuple2Rdd.flatMap(t2 => {
      Array(
        ((t2._1._1, GlobalConstants.VALUE_OF_ALL), t2._2),
        t2
      )
    })
    //groupRDD ((accessTime,platform),List(uid, sid,eventName),List(uid, sid,eventName)....)
    val groupRDD = flatMapRDD.groupByKey()
    val tuple2Array = groupRDD.map(t2 => {
      var newUsercount = 0
      var pageViewCount = 0
      val uidset = mutable.Set[String]()
      val sidset = mutable.Set[String]()
      t2._2.foreach(t3 => {
        val uid = t3._1
        val sid = t3._2
        uidset.add(uid)
        sidset.add(sid)
        if (t3._3.equals(EventEnum.LAUNCH_EVENT.toString)) newUsercount += 1
        if (t3._3.equals(EventEnum.BROWSER_PRODUCT_EVENT.toString) || t3._3.equals(EventEnum.BROWSER_PRODUCT_EVENT.toString)) pageViewCount += 1
      })
      val session_count = sidset.size
      val active_users = uidset.size
      val new_install_users = newUsercount
      val session_length = pageViewCount / session_count
      (t2._1, (active_users, new_install_users, session_count, session_length))
    }).collect()
    val connection = JdbcHelper.getConnection()
    val statsUserArray = tuple2Array.map(t2 => {
      //t2==>((accessTime,platform),(active_users, new_install_users, session_count, session_length))
      val date_dimension_id = DimensionDao.getDimensionId(DateDimension.buildDateDimension(t2._1._1), connection)
      val platform_dimension_id = DimensionDao.getDimensionId(new PlatformDimension(0, t2._1._2), connection)
      val active_users: Int = t2._2._1
      val new_install_users: Int = t2._2._2
      val session_count: Int = t2._2._3
      val session_length: Int = t2._2._4
      val created: String = t2._1._1
      new StatsUser(date_dimension_id, platform_dimension_id, active_users, new_install_users, session_count, session_length, created)
    })
    if (connection != null)
      connection.close()

    //将结果保存到mysql表中
    StatsUserDao.deleteByDateDimensionId(statsUserArray(0).date_dimension_id)
    StatsUserDao.insertBatch(statsUserArray)
  }

  /**
    *
    * @param eventLogRDD
    * (uid, sid, accessTime, eventName, country, province, city, platform, browserName, productId, osName)
    * @return
    */
  def statsDeviceLocation(eventLogRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String)]) = {
    val tuple2Rdd = eventLogRDD.map(t11 => ((Utils.formatDate(t11._3.toLong, "yyyy-MM-dd"), t11._8, t11._5, t11._6, t11._7), (t11._1, t11._2)))
    val flatMaprdd = tuple2Rdd.flatMap(t2 => {
      Array(
        //所有平台，国家
        ((t2._1._1, GlobalConstants.VALUE_OF_ALL, t2._1._3, GlobalConstants.VALUE_OF_ALL, GlobalConstants.VALUE_OF_ALL), t2._2),
        //所有平台，省
        ((t2._1._1, GlobalConstants.VALUE_OF_ALL, t2._1._3, t2._1._4, GlobalConstants.VALUE_OF_ALL), t2._2),
        //所有平台，市
        ((t2._1._1, GlobalConstants.VALUE_OF_ALL, t2._1._3, t2._1._4, t2._1._5), t2._2),
        //具体平台，国家
        ((t2._1._1, t2._1._2, t2._1._3, GlobalConstants.VALUE_OF_ALL, GlobalConstants.VALUE_OF_ALL), t2._2),
        //具体平台，省
        ((t2._1._1, t2._1._2, t2._1._3, t2._1._4, GlobalConstants.VALUE_OF_ALL), t2._2),
        //具体平台，市
        t2
      )
    })
    val groupbyrdd = flatMaprdd.groupByKey()
    val resRdd = groupbyrdd.map(g => {
      val uidset = mutable.Set[String]()
      val sidmap = mutable.Map[String, Int]()
      g._2.foreach(t2 => {
        val uid = t2._1
        val sid = t2._2
        uidset.add(uid)
        sidmap.put(sid, sidmap.getOrElse(sid, 0) + 1)
      })
      val active_users = uidset.size

      val session_count = sidmap.size
      val bounce_sessions = sidmap.filter(x => (x._2 == 1)).size
      (g._1, active_users, session_count, bounce_sessions)
    }).collect()
    val connection = JdbcHelper.getConnection()
    val statsDeviceLocationArray = resRdd.map(t4 => {
      val date_dimension_id = DimensionDao.getDimensionId(DateDimension.buildDateDimension(t4._1._1), connection)
      val platform_dimension_id = DimensionDao.getDimensionId(new PlatformDimension(0, t4._1._2), connection)
      val location_dimension_id = DimensionDao.getDimensionId(new LocationDimension(0, t4._1._3, t4._1._4, t4._1._5), connection)
      val active_users = t4._2
      val session_count = t4._3
      val bounce_sessions = t4._4
      val created = t4._1._1
      new StatsDeviceLocation(date_dimension_id, platform_dimension_id, location_dimension_id, active_users, session_count, bounce_sessions, created)
    })
    if (connection != null)
      connection.close()
    StatsDeviceLocationDao.deleteByDateDimensionId(statsDeviceLocationArray(0).date_dimension_id)
    StatsDeviceLocationDao.insertBatch(statsDeviceLocationArray)
  }

  /**
    * 同一天同一个平台每个事件发生的总次数
    *
    * @param eventLogRDD
    * (uid, sid, accessTime, eventName, country, province, city, platform, browserName, productId, osName)
    * @return
    */
  def Demo(eventLogRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String)]) = {
    val t2Rdd = eventLogRDD.map(t11 => ((Utils.formatDate(t11._3.toLong, "yyyy-MM-dd"), t11._8, t11._4), t11._4))
    val flatmapRdd = t2Rdd.flatMap(t2 => {
      Array(
        //同一天，所有平台，所有事件
        ((t2._1._1, GlobalConstants.VALUE_OF_ALL, GlobalConstants.VALUE_OF_ALL), t2._2),
        //同一天，具体平台，所有事件
        ((t2._1._1, t2._1._2, GlobalConstants.VALUE_OF_ALL), t2._2),
        //同一天，所有平台，具体事件
        ((t2._1._1, GlobalConstants.VALUE_OF_ALL, t2._1._3), t2._2),
        t2
      )
    }).reduceByKey(_ + _)

  }

  /**
    * 统计同一天同一地区浏览次数排名前3的商品
    *
    * @param eventLogRDD
    * (uid, sid, accessTime, eventName, country, province, city, platform, browserName, productId, osName)
    */
  def areaBrowserProductTop3Stats(eventLogRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String)]) = {
    val tuple5Rdd = eventLogRDD.filter(x => x._4.equals(EventEnum.BROWSER_PRODUCT_EVENT.toString) && StringUtils.isNotBlank(x._10))
      .map(t11 => Row(Utils.formatDate(t11._3.toLong, "yyyy-MM-dd"), t11._5, t11._6, t11._7, t11._10))
    /**
      * rdd转换成dataframe有两种方式
      * dataframe=rdd+schema
      * 这个rdd里面的数据类型必须是一个行对象row
      * 1，通过反射推断每一列的列名和列的数据类型
      * 2，自定义元数据（指定了列名和列的数据类型）
      */
    val schema = StructType(
      List(
        StructField("date", StringType, false),
        StructField("country", StringType, false),
        StructField("province", StringType, false),
        StructField("city", StringType, false),
        StructField("product_id", StringType, false)
      )
    )
    /**
      * 几个方法的解释
      * createOrReplaceTempView : 创建了一个局部视图，如果在当前上下文中存在相同名称的视图，那么就替换这个视图
      * createTempView:创建了一个局部视图，如果在当前上下文中存在相同名称的视图，那么就抛异常
      *
      * createOrReplaceGlobalTempView: 创建了一个全局视图，如果在当前上下文中存在相同名称的视图，那么就替换这个视图
      * createGlobalTempView:创建了一个全局视图，如果在当前上下文中存在相同名称的视图，那么就抛异常
      */
    //将rdd和schema进行关联
    spark.createDataFrame(tuple5Rdd, schema).createOrReplaceTempView("area_browser_product_view")
    spark.sql(
      """
        |select date,country,province,city,product_id,count(product_id) browser_count from area_browser_product_view
        |group by date,country,province,city,product_id
      """.stripMargin).createOrReplaceTempView("area_browser_product_count_view")
    //注册用户自定义聚合函数
    spark.udf.register("city_concat_func", new CityConcatUDAF)
    val df = spark.sql(
      """
select date,country,province,product_id,browser_count,city_infos from (
select row_number() over(partition by  date,country,province order by browser_count desc) rank, date,country,province
,product_id,browser_count,city_infos
from(
select date,country,province,product_id,sum(browser_count) browser_count,city_concat_func(city) city_infos
from area_browser_product_count_view group by
date,country,province,product_id) temp) tmp where rank<=3

      """.stripMargin)
    //    df.printSchema()
    val connection = JdbcHelper.getConnection()
    val areaTop3ProductArray = df.collect().map(row => {
      val date_dimension_id = DimensionDao.getDimensionId(DateDimension.buildDateDimension(row.getAs[String]("date")), connection)
      val location_dimension_id = DimensionDao.getDimensionId(new LocationDimension(0, row.getAs[String]("country"), row.getAs[String]("province"), GlobalConstants.VALUE_OF_ALL), connection)
      val product_id: Long = row.getAs[String]("product_id").toLong
      val browser_count = row.getAs[Long]("browser_count")
      val city_infos = row.getAs[String]("city_infos")
      new AreaTop3Product(date_dimension_id, location_dimension_id, product_id, browser_count, city_infos)
    })
    if (connection != null) connection.close()
    AreaTop3ProductDao.deleteByDateDimensionId(areaTop3ProductArray(0).date_dimension_id)
    AreaTop3ProductDao.insertBatch(areaTop3ProductArray)
  }

  def main(args: Array[String]): Unit = {
    //1,验证参数是否正确
    validateInputArgs(args)
    //2,从hbase中加载指定日期的日志
    val eventLogRDD = loadDataFromHbase(inputDate)
    eventLogRDD.cache()
    //3,按时间和平台维度对我们的session进行统计分析，将结果保持到mysql表中
    //    sessionVisitTimeAndStepLengthAnalysisStat(eventLogRDD)
    //统计同一天同一个平台活跃用户数，新增用户数，会话个数
//        userStats(eventLogRDD)
    //    statsDeviceLocation(eventLogRDD)
    //    Demo(eventLogRDD)
    areaBrowserProductTop3Stats(eventLogRDD)
    sc.stop()
  }
}
