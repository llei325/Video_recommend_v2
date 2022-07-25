package com.imooc.flink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase, Transaction, TransactionWork}

/**
 * 维护粉丝数据在Neo4j中的关注关系
 * Created by lei
 */
class Neo4jSink extends RichSinkFunction[Tuple3[String,String,String]]{
  //保存neo4j相关的配置参数
  var param: Map[String,String] = Map()

  var driver: Driver = _

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
   * 初始化方法，只执行一次
   * 适合初始化资源连接
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {
    this.driver = GraphDatabase.driver(param("boltUrl"), AuthTokens.basic(param("userName"), param("passWord")))
  }

  /**
   * 核心代码，来一条数据，此方法会执行一次
   * @param value
   * @param context
   */
  override def invoke(value: (String, String, String), context: SinkFunction.Context[_]): Unit = {
    //开启会话
    val session = driver.session()
    val followType = value._1
    val followerUid = value._2
    val followUid = value._3

    if("follow".equals(followType)){
      //添加关注：因为涉及多条命令，所以需要使用事务
      session.writeTransaction(new TransactionWork[Unit](){
        override def execute(tx: Transaction): Unit = {
          try{
            tx.run("merge (:User {uid:'"+followerUid+"'})")
            tx.run("merge (:User {uid:'"+followUid+"'})")
            tx.run("match (a:User {uid:'"+followerUid+"'}),(b:User {uid:'"+followUid+"'}) merge (a) -[:follow]-> (b)")
            tx.commit()
          }catch {
            case ex: Exception => tx.rollback()
          }
        }
      })
    }else{
      //取消关注
      session.run("match (:User {uid:'"+followerUid+"'}) -[r:follow]-> (:User {uid:'"+followUid+"'}) delete r")
    }
    //关闭会话
    session.close()
  }

  /**
   * 任务停止的时候会先调用此方法
   * 适合关闭资源连接
   */
  override def close(): Unit = {
    //关闭连接
    if(driver!=null){
      driver.close()
    }
  }
}
