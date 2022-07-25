package com.imooc.flink

import org.apache.flink.api.scala.ExecutionEnvironment
import redis.clients.jedis.Jedis

/**
 * 任务7：
 * 将三度列表关系数据导出到Redis
 * Created by lei
 */
object ExportDataScala {
  def main(args: Array[String]): Unit = {
    var filePath = "hdfs://bigdata01:9000/data/recommend_data/20260125"
    var redisHost = "bigdata04"
    var redisPort = 6379
    if(args.length>0){
      filePath = args(0)
      redisHost = args(1)
      redisPort = args(2).toInt
    }

    val env = ExecutionEnvironment.getExecutionEnvironment
    //读取hdfs中的数据
    val text = env.readTextFile(filePath)

    //添加隐式转换代码
    import org.apache.flink.api.scala._
    //处理数据
    text.mapPartition(it=>{
      //获取jedis连接
      val jedis = new Jedis(redisHost, redisPort)
      //开启管道
      val pipeline = jedis.pipelined()
      it.foreach(line=>{
        val fields = line.split("\t")
        //获取uid
        val uid = fields(0)
        //获取待推荐主播列表
        val recommend_uids = fields(1).split(",")

        //注意：在这里给key起一个有意义的名字，l表示list类型、rec是recommend的简写
        val key = "l_rec_"+uid

        //先删除(保证每周更新一次),pipeline中的删除操作在scala语言下使用有问题
        jedis.del(key)
        for(r_uid <- recommend_uids){
          pipeline.rpush(key,r_uid)
          //给key设置一个有效时间，30天，如果30天数据没有更新，则删除此key
          pipeline.expire(key,30*24*60*60)
        }
      })
      //提交管道中的命令
      pipeline.sync()
      //关闭jedis连接
      jedis.close()
      ""
    }).print()
  }

}
