#!/bin/bash
#默认获取上周一的时间
dt=`date -d "7 days ago" +"%Y%m%d"`
if [ "x$1" != "x" ]
then
    dt=`date -d "7 days ago $1" +"%Y%m%d"`
fi

#HDFS输入数据路径
filePath="hdfs://bigdata01:9000/data/recommend_data/${dt}"

masterUrl="yarn-cluster"
appName="ExportDataScala"`date +%s`
redisHost="bigdata04"
redisPort=6379




#注意：需要将flink脚本路径配置到linux的环境变量中
flink run \
-m ${masterUrl} \
-ynm ${appName} \
-yqu default \
-yjm 1024 \
-ytm 1024 \
-ys 1 \
-p 5 \
-c com.imooc.flink.ExportDataScala \
/data/soft/video_recommend_v2/jobs/export_data-1.0-SNAPSHOT-jar-with-dependencies.jar ${filePath} ${redisHost} ${redisPort}

#验证任务执行状态
appStatus=`yarn application -appStates FINISHED -list | grep ${appName} | awk '{print $8}'`
if [ "${appStatus}" != "SUCCEEDED" ]
then
    echo "任务执行失败"
    # 发送短信或者邮件
else
    echo "任务执行成功"
fi