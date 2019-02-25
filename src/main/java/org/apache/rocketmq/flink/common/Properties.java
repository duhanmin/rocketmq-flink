package org.apache.rocketmq.flink.common;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.flink.RocketMQConstant;
import org.apache.rocketmq.flink.RocketMQSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Properties {
    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSource.class);
    public static JSONObject convertToJSONObject(JSONObject map,MessageQueue mq,long offset,long bornTimestamp) {
        try {
            map.put(RocketMQConstant.MQ_TOPIC,mq.getTopic());
            map.put(RocketMQConstant.MQ_QUEUE_ID,mq.getQueueId());
            map.put(RocketMQConstant.MQ_BROKER_NAME,mq.getBrokerName());
            map.put(RocketMQConstant.MQ_QUEUE_OFFSET,offset);
            map.put(RocketMQConstant.MQ_BORN_TIMESTAMP,bornTimestamp);
        } catch (Exception e) {
            LOG.error("对象转json失败！！！", e);
        }
        return map;
    }
}
