package task.analysis

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * Created by Administrator on 2019/5/6.
  */
class CityConcatUDAF extends UserDefinedAggregateFunction {
    /**
      * 确定输入值的数据类型
      *
      * @return
      */
    override def inputSchema: StructType = {
      StructType(List(StructField("city", StringType, true)))
    }

    /**
      * 每次聚合完后的数据类型
      *
      * @return
      */
    override def bufferSchema: StructType = {
      StructType(List(StructField("city_infos", StringType, true)))
    }

    /**
      * 最终结果的类型
      *
      * @return
      */
    override def dataType: DataType = {
      StringType
    }

    /**
      * 输入的数据类和返回的结果的数据类型是否一致
      *
      * @return
      */
    override def deterministic: Boolean = {
      true
    }
    /**
      * 聚合的初始值或归并的初始值
      *
      * @param buffer
      */
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = ""
    }

    /**
      * 定义每次如何进行聚合的逻辑
      *
      * @param buffer 聚合的初始值或聚合后的结果
      * @param input  需要聚合的值
      */
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      //先取 再聚合 后放回
      var newValue = buffer(0) + "," + input(0)
      if (newValue.startsWith(",")) {
        newValue = newValue.substring(1)
      }
      buffer.update(0, newValue)
    }

    /**
      * 定义了如何进行归并的逻辑
      * 由于spark是分布式计算，意味着我们的结果可能散落在多个分区上，所以需要对多个分区上的数据进行归并
      *
      * @param buffer1 归并的初始值或归并后的结果
      * @param buffer2 需要进行归并的分区的值
      */
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      //先取 再聚合 后放回
      var newValue = buffer1(0) + "," + buffer2(0)
      if (newValue.startsWith(",")) {
        newValue = newValue.substring(1)
      }
      buffer1(0) = newValue
    }

    /**
      * 返回最终聚合的结果
      *
      * @param buffer
      * @return
      */
    override def evaluate(buffer: Row): Any = {
      buffer(0)
    }
}
