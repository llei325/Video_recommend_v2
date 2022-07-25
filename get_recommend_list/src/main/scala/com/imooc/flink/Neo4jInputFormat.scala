package com.imooc.flink

import org.apache.flink.api.common.io.statistics.BaseStatistics
import org.apache.flink.api.common.io.{DefaultInputSplitAssigner, NonParallelInput, RichInputFormat}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.io.{GenericInputSplit, InputSplit, InputSplitAssigner}
import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase, Result, Session}

/**
 * 从Neo4j中查询满足条件的主播
 * 一周内活跃过，并且主播等级大于4
 * Created by lei
 */
class Neo4jInputFormat extends RichInputFormat[String,InputSplit] with NonParallelInput{
  //注意：with NonParallelInput 表示此组件不支持多并行度

  //保存neo4j相关的配置参数
  var param: Map[String,String] = Map()

  var driver: Driver = _
  var session: Session = _
  var result: Result = _

  /**
   * 构造函数
   * 接收neo4j相关的配置参数
   * @param param
   */
  def this(param: Map[String,String]){
    this()
    this.param = param
  }

  /**
   * 配置此输入格式
   * @param parameters
   */
  override def configure(parameters: Configuration): Unit = {}

  /**
   * 获取输入数据的基本统计信息
   * @param cachedStatistics
   * @return
   */
  override def getStatistics(cachedStatistics: BaseStatistics): BaseStatistics = {
    cachedStatistics
  }

  /**
   * 对输入数据切分split
   * @param minNumSplits
   * @return
   */
  override def createInputSplits(minNumSplits: Int): Array[InputSplit] = {
    Array(new GenericInputSplit(0,1))
  }

  /**
   * 获取切分的split
   * @param inputSplits
   * @return
   */
  override def getInputSplitAssigner(inputSplits: Array[InputSplit]): InputSplitAssigner = {
    new DefaultInputSplitAssigner(inputSplits)
  }

  /**
   * 初始化方法：只执行一次
   * 获取neo4j连接，开启会话
   */
  override def openInputFormat(): Unit = {
    //初始化Neo4j连接
    this.driver = GraphDatabase.driver(param("boltUrl"), AuthTokens.basic(param("userName"), param("passWord")))
    //开启会话
    this.session = driver.session()
  }

  /**
   * 关闭Neo4j连接
   */
  override def closeInputFormat(): Unit = {
    if(driver!=null){
      driver.close()
    }
  }

  /**
   * 此方法也是只执行一次
   * @param split
   */
  override def open(split: InputSplit): Unit = {
    this.result = session.run("match (a:User) where a.timestamp >="+param("timestamp")+" and a.level >= "+param("level")+" return a.uid")
  }

  /**
   * 如果数据读取完毕号以后，需要返回true
   * @return
   */
  override def reachedEnd(): Boolean = {
    !result.hasNext
  }

  /**
   * 读取结果数据，一次读取一条
   * @param reuse
   * @return
   */
  override def nextRecord(reuse: String): String = {
    val record = result.next()
    val uid = record.get(0).asString()
    uid
  }

  /**
   * 关闭会话
   */
  override def close(): Unit = {
    if(session!=null){
      session.close()
    }
  }
}
