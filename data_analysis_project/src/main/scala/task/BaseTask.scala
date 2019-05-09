package task


import Util.Utils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2019/4/25.
  */
trait BaseTask {
  val configuration = new Configuration()
  configuration.addResource("hbase-site.xml")
  val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
  //调整spark-sql在shuffle阶段的任务并行度，默认是200
  conf.set("spark.sql.shuffle.partitions", "1")
  val spark = SparkSession.builder().config(conf).getOrCreate()
  val sc = spark.sparkContext
  var inputDate: String = null

  /**
    * 验证参数是否正确
    * 验证参数的个数>1
    * 验证参数的格式 yyyy-MM-dd
    *
    * @param args
    */
  def validateInputArgs(args: Array[String]) = {
    if (args.length == 0) {
      throw new SparkException(
        """
          |Usage:etl.AnalysisLogTask
          |errorMessage:任务至少需要有一个日期参数
        """.stripMargin)
    }
    if (!Utils.validateInputDate(args(0))) {
      throw new SparkException(
        """
          |Usage:etl.AnalysisLogTask
          |errorMessage:，任务第一个参数是一个日期日期的格式是：yyyy-MM-dd
        """.stripMargin)
    }
    inputDate = args(0)
  }
}
