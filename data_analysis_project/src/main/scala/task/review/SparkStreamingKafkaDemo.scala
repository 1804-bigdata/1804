package task.review

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2019/5/8.
  */
object SparkStreamingKafkaDemo {
  /**
    * Receive 模式
    */
  def receiverDemo() = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(2))
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> "hadoop1:2181,hadoop2:2181",
      "group.id" -> "g1804",

      /**
        * 当在zk上找不到消费偏移量时，这个配置项就会起作用，代表的是起始消费位置
        * 1,smallest :最早消息的偏移量
        * 2,largest：最新消息的偏移量(默认值)
        */
      "auto.offset.reset" -> "largest"
      //每隔多长时间更新一次消费偏移量(默认是60s)
      // "auto.commit.interval.ms" -> "1000"
    )
    //1，创建初始的dstream
    val inputDstream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Map("wordcount" -> 3), StorageLevel.MEMORY_ONLY)
    //2，对初始的dstream进行不断的转换操作
    //需要注意的是，我们消费kafak里面的数据，取出来的数据是（key,value），value才是我们的消息
    val wordCountDstream = inputDstream.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //3，进行输出
    wordCountDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * Direct模式1（直联）
    */
  def directDemo1() = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(2))
    val kafkaParams = Map[String, String](
      //kafka broker地址
      "metadata.broker.list" -> "hadoop1:9092,hadoop2:9092",
      "group.id" -> "G1804",
      "auto.offset.reset" -> "largest"
    )
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("wordcount")).map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * Direct模式2（直联）
    */
  //  def directDemo2() = {
  //    val checkpointPath = "/checkpoint/G1804"
  //    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
  //    val sc = new SparkContext(conf)
  //    sc.setLogLevel("WARN")
  //    /**
  //      * checkpointPath保存了一下基本信息：
  //      * 1，streaming 应用的配置信息 sparkConf
  //      * 2,保存了dstream的操作转换逻辑关系
  //      * 3,保存了未处理完的batch元数据信息
  //      * 4,保存了消费偏移量
  //      */
  //    val ssc = StreamingContext.getOrCreate(checkpointPath, () => stramingcontext(sc, checkpointPath))
  //    ssc.start()
  //    ssc.awaitTermination()
  //  }

  //  def stramingcontext(sc: SparkContext, checkpointPath: String): StreamingContext = {
  //    val ssc = new StreamingContext(sc, Seconds(2))
  //    ssc.checkpoint(checkpointPath)
  //    val kafkaParams = Map[String, String](
  //      "metadata.broker.list" -> "hadoop1:2181,hadoop2:2181",
  //      "group.id" -> "G1804",
  //      "auto.offset.reset" -> "largest"
  //    )
  //    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("wordcount")).map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()
  //    ssc
  //  }
  def main(args: Array[String]): Unit = {
    //    receiverDemo()
    directDemo1()
    //    directDemo2()
  }
}
