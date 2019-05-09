package kafka

import java.util.concurrent.atomic.AtomicReference

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.SparkException
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * Created by Administrator on 2019/5/8.
  */
class KafkaManager(val kafkaPrams: Map[String, String], topics: Set[String]) extends Serializable {
  //kafka客户端的api
  val kafkaCluster = new KafkaCluster(kafkaPrams)
  //程序启动后立即调用，用来设置或更新消费者偏移量
  setOrUpdateOffset()

  def setOrUpdateOffset() = {
    //默认有消费偏移量
    var isConsume = true
    //获取topic分区元数据信息
    val topicAndPartitions = getPartitions()
    //通过分区信息，查找对应的消费者偏移量
    val errOrConsumerOffsets = kafkaCluster.getConsumerOffsets(kafkaPrams("group.id"), topicAndPartitions)
    //判断是否有消费者偏移量
    if (errOrConsumerOffsets.isLeft) {
      isConsume = false
    }
    /** 三种模式
      * 1.找不到消费者偏移量 "auto.offset.reset"->"largest"   那么这个map存放的是每个分区最新消息的偏移量，作为初始化的消费偏移量
      * 2.找不到消费者偏移量 "auto.offset.reset"->"smallest" 那么这个map存放的是每个分区最早消息的偏移量，作为初始化的消费偏移量
      * 3.找到消费者偏移量    并且有的分区消费偏移量过期了，那么这个map里面存放的是更新后的消费偏移量
      */
    val setOrUpdateMap = mutable.Map[TopicAndPartition, Long]()
    if (isConsume) {
      //有消费者偏移量
      //取出消费者偏移量
      val consumeOffsetsMap: Map[TopicAndPartition, Long] = errOrConsumerOffsets.right.get
      //取出每个分区最早消息偏移量
      val errOrlatestLeaderOffsetsMap = getEarliestLeaderOffsets(topicAndPartitions)
      consumeOffsetsMap.foreach(t2 => {
        //当前分区
        val topicAndPartition = t2._1
        //当前分区消费偏移量
        val consumeoffset = t2._2
        //当前分区最早的消费者偏移量
        val earliestLeaderOffset = errOrlatestLeaderOffsetsMap(topicAndPartition).offset
        if (consumeoffset < earliestLeaderOffset) {
          //消费者偏移量过期了
          setOrUpdateMap.put(topicAndPartition, earliestLeaderOffset)
        }
      })

    } else {
      //没有消费偏移量
      val offsetReset = kafkaPrams.getOrElse("auto.offset.reset", "largest")
      if (offsetReset.equals("smallest")) {
        val earliestLeaderOffsetsMap = getEarliestLeaderOffsets(topicAndPartitions)
        earliestLeaderOffsetsMap.foreach(t2 => {
          val topicAndPartition = t2._1
          val earliestLeaderOffset = t2._2.offset
          setOrUpdateMap.put(topicAndPartition, earliestLeaderOffset)
        })
      } else {
        val latestLeaderOffsetsMap = getLatestLeaderOffsets(topicAndPartitions)
        latestLeaderOffsetsMap.foreach(t2 => {
          //当前分区
          val topicAndPartition = t2._1
          //当前分区最新消息的偏移量
          val latestLeaderOffset = t2._2.offset
          setOrUpdateMap.put(topicAndPartition, latestLeaderOffset)
        })
      }
    }
    //设置或更新偏移量
    kafkaCluster.setConsumerOffsets(kafkaPrams("group.id"), setOrUpdateMap.toMap)
  }

  /**
    * 获取消费的偏移量
    */
  def getConsumeOffsets(): Map[TopicAndPartition, Long] = {
    //尝试获取消费偏移量
    val errOrConsumerOffsets = kafkaCluster.getConsumerOffsets(kafkaPrams("group.id"), getPartitions())
    if (errOrConsumerOffsets.isLeft) {
      throw new SparkException(
        """
          |kafka.KafkaManager
          |message:尝试获取分区消费偏移量失败
        """.stripMargin)
    }
    errOrConsumerOffsets.right.get
  }

  //保存rdd的消费偏移量
  val offsetRangesAtomicReference = new AtomicReference[Array[OffsetRange]]()

  /**
    * 创建输入的Dstream
    */
  def createDirectDstreams[
  K: ClassTag, //key 的类型
  V: ClassTag, //value 的类型
  KD <: Decoder[K] : ClassTag, //key 的反序列化类
  VD <: Decoder[V] : ClassTag, //value 的反序列化类
  R: ClassTag] //返回值类型
  (ssc: StreamingContext) = {
    //取出保存在第三方的消费偏移量
    val fromOffsets = getConsumeOffsets()
    //打印日志（起始消费位置）
    printFromOffsets(fromOffsets)
    //创建输入的Dstream
    val kafkaInputDstream = KafkaUtils.createDirectStream[K, V, KD, VD, R](ssc, kafkaPrams, fromOffsets,
      (messageAndMetaData: MessageAndMetadata[K, V]) => {
        messageAndMetaData.message().asInstanceOf[R]
      }
    ).transform(rdd => {
      //获取当前rdd的消费偏移量
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRangesAtomicReference.set(offsetRanges)
      rdd
    })
    kafkaInputDstream
  }

  /**
    * 更新偏移量
    */
  def updateConsumerOffsets() = {
    val offsetRanges = offsetRangesAtomicReference.get()
    println("***************************************")
    offsetRanges.foreach(t2 => {
      //当前的分区
      val topicAndPartition = t2.topicAndPartition()
      //结束的消费位置
      val untilOffset = t2.untilOffset
      val offset = Map[TopicAndPartition, Long](topicAndPartition -> untilOffset)
      println(s"正在更新消费偏移量【topic:${topicAndPartition.topic}   partition:${topicAndPartition.partition}    untilOffset:${offset}】")
      //更新当前分区
      kafkaCluster.setConsumerOffsets(kafkaPrams("group.id"), offset)
    })
    println("*****************************************")
  }

  /**
    * 打印起始消费位置
    *
    * @param fromOffsets
    */
  def printFromOffsets(fromOffsets: Map[TopicAndPartition, Long]) = {
    println("==================================================================")
    fromOffsets.foreach(t2 => {
      println(s"【topic:${t2._1.topic} partition:${t2._1.partition} fromOffsets:${t2._2}】")
    })
    println("==================================================================")

  }

  def getPartitions(): Set[TopicAndPartition] = {
    //尝试获取分区的元数据信息
    val errOrPartitions = kafkaCluster.getPartitions(topics)
    //判断是否获取到了
    if (errOrPartitions.isLeft) {
      throw new SparkException(
        """
          |kafka.KafkaManager
          |message:尝试获取分区元数据失败
        """.stripMargin)
    }
    //取出分区元数据信息
    errOrPartitions.right.get
  }

  /**
    * 获取每个分区的最早的消息偏移量
    *
    * @param topicAndPartitions
    * topic分区元数据信息
    * @return 每个分区最早消息偏移量
    */
  def getEarliestLeaderOffsets(topicAndPartitions: Set[TopicAndPartition]) = {
    //通过分区信息，尝试获取每个分区最早消息偏移量
    val errOrEarliestLeaderOffsets = kafkaCluster.getEarliestLeaderOffsets(topicAndPartitions)
    if (errOrEarliestLeaderOffsets.isLeft) {
      throw new SparkException(
        """
          |kafka.KafkaManager
          |message:尝试获取每个分区最新消息偏移量失败
        """.stripMargin)
    }
    errOrEarliestLeaderOffsets.right.get
  }

  /**
    * 获取每个分区的最新的消息偏移量
    *
    * @param topicAndPartitions topic分区元数据信息
    * @return 分区的最新的消息偏移量
    */
  def getLatestLeaderOffsets(topicAndPartitions: Set[TopicAndPartition]) = {
    val errOrlatestLeaderOffsets = kafkaCluster.getLatestLeaderOffsets(topicAndPartitions)
    if (errOrlatestLeaderOffsets.isLeft) {
      throw new SparkException(
        """
          |kafka.KafkaManager
          |message:尝试获取每个分区最新消息偏移量失败
        """.stripMargin)
    }
    errOrlatestLeaderOffsets.right.get
  }
}
