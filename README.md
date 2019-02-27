# RocketMQ-Flink

RocketMQ integration for [Apache Flink](https://flink.apache.org/). This module includes the RocketMQ source and sink that allows a flink job to either write messages into a topic or read from topics in a flink job.

相较于[rocketmq-externals/rocketmq-flink](https://github.com/duhanmin/rocketmq-externals/tree/master/rocketmq-flink) 版本对比：

* 优化：消息传输byte[]以nio方式读取数据
* 新增功能：实现指定offset定点位置启动
* 修复bug：自带checkpoint灵活性差，且会导致重新配置消费旧数据失效

# 指定位置

代码
```java
consumerProps.setProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO, RocketMQConfig.CONSUMER_OFFSET_SITE);
consumerProps.setProperty(RocketMQConfig.CONSUMER_OFFSET_SITE_STARTING_OFFSETS, "{\"2\":{\"iZbp1f9edjszup3fshxxheZ\":{\"0\":2,\"1\":3,\"2\":4,\"3\":3,\"4\":4,\"5\":3,\"6\":4,\"7\":5}}}");
```

配置
```json
{
    "2": {
        "iZbp1f9edjszup3fshxxheZ": {
            "0": 2,
            "1": 3,
            "2": 4,
            "3": 3,
            "4": 4,
            "5": 3,
            "6": 4,
            "7": 5
        }
    }
}
```

# 问题
测试发现，部分分区可能会出现丢失

# 赞助
<img src="https://github.com/duhanmin/mathematics-statistics-python/blob/master/images/90f9a871536d5910cad6c10f0297fc7.jpg" width="250">
