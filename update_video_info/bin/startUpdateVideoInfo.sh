#!/bin/bash
#获取最近一个月的文件目录
#filepath=""
#for((i=1;i<=30;i++))
#do
#    filepath+="hdfs://bigdata01:9000/data/video_info/"`date -d "$i days ago" +"%Y%m%d"`,
#done
#注意：在使用的时候需要将最后面的逗号去掉 ${filePath:0:-1}


#默认获取昨天时间
dt=`date -d "1 days ago" +"%Y%m%d"`
if [ "x$1" != "x" ]
then
dt=$1
fi

#HDFS输入数据路径
filePath="hdfs://bigdata01:9000/data/video_info/${dt}"

masterUrl="yarn-cluster"
appName="UpdateVideoInfoScala"`date +%s`
boltUrl="bolt://bigdata04:7687"
userName="neo4j"
passWord="admin"

#注意：需要将flink脚本路径配置到linux的环境变量中
flink run \
-m ${masterUrl} \
-ynm ${appName} \
-yqu default \
-yjm 1024 \
-ytm 1024 \
-ys 1 \
-p 5 \
-c com.imooc.flink.UpdateVideoInfoScala \
/data/soft/video_recommend_v2/jobs/update_video_info-1.0-SNAPSHOT-jar-with-dependencies.jar ${filePath} ${boltUrl} ${userName} ${passWord}

#验证任务执行状态
appStatus=`yarn application -appStates FINISHED -list | grep ${appName} | awk '{tmp=$8;getline;print tmp","$8}'`
if [ "${appStatus}" != "SUCCEEDED,SUCCEEDED" ]
then
    echo "任务执行失败"
    # 发送短信或者邮件
else
    echo "任务执行成功"
fi