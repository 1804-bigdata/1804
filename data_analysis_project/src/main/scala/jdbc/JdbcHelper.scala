package jdbc

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}
import config.ConfigurationManager
import constants.GlobalConstants

/**
  * Created by Administrator on 2019/4/29.
  */
object JdbcHelper {
  /**
    * 获取jbdc连接对象
    */
  def getConnection() = {
    var connection: Connection = null
    try {
      val driver = ConfigurationManager.getvalue(GlobalConstants.JDBC_DRIVER)
      val url = ConfigurationManager.getvalue(GlobalConstants.JDBC_URL)
      val user = ConfigurationManager.getvalue(GlobalConstants.JDBC_USER)
      val password = ConfigurationManager.getvalue(GlobalConstants.JDBC_PASSWORD)
      //注册mysql数据库驱动
      Class.forName(driver)
      connection = DriverManager.getConnection(url, user, password)
    } catch {
      case e: SQLException => e.printStackTrace
      case e: Exception => e.printStackTrace
    }
    connection
  }

  /**
    * 一条记录的增，删，改方法
    *
    * @param sql
    * insert into student(id,name,age)values(?,?,?)
    * @param sqlParams
    * Array(1,xm,13)
    */
  def executeUpdate(sql: String, sqlParams: Array[Any]) = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    try {
      //获取jdbc连接对象
      connection = getConnection()
      //创建sql语句对象
      preparedStatement = connection.prepareStatement(sql)
      //给sql语句进行赋值
      for (i <- 0 until (sqlParams.length)) {
        preparedStatement.setObject(i + 1, sqlParams(i))
      }
      //执行sql语句
      preparedStatement.executeUpdate()
    } catch {
      case e: SQLException => e.printStackTrace()
    } finally {
      if (preparedStatement != null)
        preparedStatement.close()
      if (connection != null)
        connection.close()
    }
  }

  /**
    * 多条记录的增，删，改批处理方法
    *
    * @param sql
    * insert into student(id,name,age)values(?,?,?)
    * @param sqlParamsArray
    * Array( Array(1,xm,13), Array(1,xm,13), Array(1,xm,13),...)
    */
  def executeBatch(sql: String, sqlParamsArray: Array[Array[Any]]) = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    try {
      //获取连接对象
      connection = getConnection()
      //设置事务为手动提交
      connection.setAutoCommit(false)
      //创建sql语句对象
      preparedStatement = connection.prepareStatement(sql)
      for (i <- 0 until (sqlParamsArray.length)) {
        val sqlParams = sqlParamsArray(i)
        for (j <- 0 until (sqlParams.length)) {
          preparedStatement.setObject(j + 1, sqlParams(j))
        }
        preparedStatement.addBatch()
      }
      //执行batch
      preparedStatement.executeBatch()
      //提交事务
      connection.commit()
    } catch {
      case e: SQLException => {
        //发生异常进行是事务回滚
        connection.rollback()
        e.printStackTrace()
      }
    } finally {
      if (preparedStatement != null)
        preparedStatement.close()
      if (connection != null)
        connection.close()
    }

  }


  def executeQuery(sql: String, sqlParams: Array[Any], f: (ResultSet) => Unit) = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      //获取连接对象
      connection = getConnection()
      //创建sql语句对象
      preparedStatement = connection.prepareStatement(sql)
      for (i <- 0 until (sqlParams.length)) {
        preparedStatement.setObject(i + 1, sqlParams(i))
      }
      resultSet = preparedStatement.executeQuery()
      f(resultSet)
    } catch {
      case e: SQLException => e.printStackTrace()
    } finally {
      if (resultSet != null)
        resultSet.close()
      if (preparedStatement != null)
        preparedStatement.close()
      if (connection != null)
        connection.close()
    }
  }
}
