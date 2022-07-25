package com.imooc.flink

import com.alibaba.fastjson.JSON
import org.apache.flink.api.scala.ExecutionEnvironment
import org.neo4j.driver.{AuthTokens, GraphDatabase}

/**
 * 任务4：
 * 每天定时更新用户活跃时间
 * Created by xuwei
 */
object UpdateUserActiveScala {
  def main(args: Array[String]): Unit = {
    var filePath = "hdfs://bigdata01:9000/data/user_active/20260201"
    var boltUrl = "bolt://bigdata04:7687"
    var userName = "neo4j"
    var passWord = "admin"
    if(args.length>0){
      filePath = args(0)
      boltUrl = args(1)
      userName = args(2)
      passWord = args(3)
    }

    val env = ExecutionEnvironment.getExecutionEnvironment
    //读取hdfs中的数据
    val text = env.readTextFile(filePath)
    //添加隐式转换代码
    import org.apache.flink.api.scala._

    //处理数据
    text.mapPartition(it=>{
      //获取neo4j的连接
      val driver = GraphDatabase.driver(boltUrl, AuthTokens.basic(userName, passWord))
      //开启一个会话
      val session = driver.session()
      it.foreach(line=>{
        val jsonObj = JSON.parseObject(line)
        val uid = jsonObj.getString("uid")
        val timeStamp = jsonObj.getString("UnixtimeStamp")
        //添加用户活跃时间
        session.run("merge (u:User {uid:'"+uid+"'}) set u.timestamp = "+timeStamp)
      })
      //关闭会话
      session.close()
      //关闭连接
      driver.close()
      ""
    }).print()
  }

}
