package task


import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2019/4/25.
  */
trait BaseTask {
  val configuration = new Configuration()
  configuration.addResource("hbase-site.xml")
  val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
  val spark = SparkSession.builder().config(conf).getOrCreate()
  val sc = spark.sparkContext
}
