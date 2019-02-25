# RocketMQ-Flink

RocketMQ integration for [Apache Flink](https://flink.apache.org/). This module includes the RocketMQ source and sink that allows a flink job to either write messages into a topic or read from topics in a flink job.

相较于[rocketmq-externals/rocketmq-flink](https://github.com/duhanmin/rocketmq-externals/tree/master/rocketmq-flink) 版本对比：

* 优化：byte[]以nio方式读取数据
* 新增功能：实现指定offset定点位置启动
* 修复bug：自带checkpoint灵活性差，且会导致重新配置消费旧数据失效

# 赞助
<img src="https://github.com/duhanmin/mathematics-statistics-python/blob/master/images/90f9a871536d5910cad6c10f0297fc7.jpg" width="250">
