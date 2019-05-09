package task.review

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2019/5/8.
  * spark-streaming 核心抽象 dstream
  * dstream:离散的数据输入流
  * spark-streaming编程步骤
  * 1，根据输入流（kafka,tcp,flume,hdfs,...），创建初始的dstream
  * 2，对初始的dstream进行不断的转换操作，形成一个又一个新的dstream
  * 3，将最后的dtream进行输出（屏幕，mysql，redis，hbase...）
  */
object StreamingDemo {
  def main(args: Array[String]): Unit = {
    /**
      * 注意，每个输入的dstream都会绑定一个receiver，这个receiver用来接收数据。receiver在接收收据的时候
      * 会独占cpu core资源，中途不会被释放，知道程序停止后。
      * local[2]在本地运行，使用两个线程模拟两个cpu core
      */
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    /**
      * 当我们通过spark ui界面观察到rdd处理时间比较长，同时在任务队列中延迟比较长。表面我们的计算能力不足
      * 即任务处理速率低于数据接收速率。此时我们可以通过限制receiver的接收数据的速率来保证streaming程序
      * 的平稳运行
      */
    //sparkConf.set("spark.streaming.receiver.maxRate", "2000")
    //从spark1.5之后，引入了反压机制，可以根据sparkstreaming任务处理速率，动态的调整receiver的接收速率
    conf.set("spark.streaming.backpressure.enabled", "true")
    //调整日志级别为警告级别
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("/checkpoint/1804")
    /**
      * 为了达到数据零丢失，我们可以开启预写日志
      * 开启预写日志，需要注意一下几点：
      * 1，必须设置checkpoint目录
      * 2，调整存储级别，防止数据冗余
      * 3，由于开启预写日志，会耗费相当一部分时间和性能进行预写日志。这样会降低receiver接收数据的速率。
      * 如果既要确保数据零丢失，又不影响receiver接收数据的速率，我们可以创建多个输入的dstream，绑定多个
      * receiver并行接收数据，然后将多个receiver并行接收到的数据进行合并，合并成一个大的dstream
      */
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    //1，根据输入流（kafka,tcp,flume,hdfs,...），创建初始的dstream
    // val lineDstream = ssc.socketTextStream("hadoop1", 9999, StorageLevel.MEMORY_AND_DISK)
    //创建多个Dstream
    val linesDstrams = (0 until (3)).map(x => ssc.socketTextStream("hadoop1", 9999, StorageLevel.MEMORY_AND_DISK))
    //调用ssc.union方法将多个dstream接收到的数据进行合并
    val lineDstream = ssc.union(linesDstrams)
    //2，对初始的dstream进行不断的转换操作，形成一个又一个新的dstream
    val wordCountDstream = lineDstream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //某个dstream被多次使用到，那么可以对这个dstream进行cache
    wordCountDstream.cache()
    /** 为防止dstream数据丢失，可以对其进行checkpoint
      * 需要注意的是，
      * 1,这里面需要我们这种checkpoint的时间间隔，这个时间间隔不能过短（因为设置
      * dstream在进行checkpoint的时候，会耗费相当一部分性能，频繁进行checkpoint会降低sparkstreaming应用
      * 处理batch的效率），spark官方建议 不要低于10秒
      * 2, 同时这个checkpoint的时间间隔必须是rdd产生时间间隔的整数倍
      * 3,必须设置一个checkpoint目录
      */
    wordCountDstream.checkpoint(Seconds(12))
    //3，将最后的dtream进行输出（屏幕，mysql，redis，hbase...）
    wordCountDstream.print()
    //启动spark-streaming应用
    ssc.start()
    //等待停止命令，结束进程，否则一直运行
    ssc.awaitTermination()
  }
}
