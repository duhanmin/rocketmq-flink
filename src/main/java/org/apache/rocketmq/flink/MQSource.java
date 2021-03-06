package org.apache.rocketmq.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.rocketmq.client.consumer.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.flink.common.Properties;
import org.apache.rocketmq.flink.common.serialization.KeyValueDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.rocketmq.flink.RocketMQConfig.*;
import static org.apache.rocketmq.flink.RocketMQConfig.CONSUMER_OFFSET_TIMESTAMP;
import static org.apache.rocketmq.flink.RocketMQUtils.getInteger;
import static org.apache.rocketmq.flink.RocketMQUtils.getLong;

public class MQSource<OUT> extends RichParallelSourceFunction<OUT> implements ResultTypeQueryable<OUT>{
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MQSource.class);

    private transient MQPullConsumerScheduleService pullConsumerScheduleService;
    private DefaultMQPullConsumer consumer;

    private KeyValueDeserializationSchema<OUT> schema;
    private RunningChecker runningChecker;
    private Map<MessageQueue, Long> offsetTable = new ConcurrentHashMap<>();
    private Boolean runOffset = Boolean.FALSE;
    private java.util.Properties props;
    private String topic;
    private String group;
    private int pullBatchSize;
    private int pullPoolSize;
    private String tag;
    private int delayWhenMessageNotFound;

    public MQSource(KeyValueDeserializationSchema<OUT> schema, java.util.Properties props) {
        this.schema = schema;
        this.props = props;
    }

    @Override
    public void open(Configuration parameters) {
        runningChecker = new RunningChecker();

        this.topic = props.getProperty(RocketMQConfig.CONSUMER_TOPIC);
        this.group = props.getProperty(RocketMQConfig.CONSUMER_GROUP);

        this.pullConsumerScheduleService = new MQPullConsumerScheduleService(group);

        this.consumer = pullConsumerScheduleService.getDefaultMQPullConsumer();
        this.consumer.setInstanceName(String.valueOf(getRuntimeContext().getIndexOfThisSubtask()));
        RocketMQConfig.buildConsumerConfigs(props, consumer);

        this.delayWhenMessageNotFound = getInteger(props, RocketMQConfig.CONSUMER_DELAY_WHEN_MESSAGE_NOT_FOUND,
                RocketMQConfig.DEFAULT_CONSUMER_DELAY_WHEN_MESSAGE_NOT_FOUND);

        this.tag = props.getProperty(RocketMQConfig.CONSUMER_TAG, RocketMQConfig.DEFAULT_CONSUMER_TAG);

        this.pullPoolSize = getInteger(props, RocketMQConfig.CONSUMER_PULL_POOL_SIZE,
                RocketMQConfig.DEFAULT_CONSUMER_PULL_POOL_SIZE);

        this.pullBatchSize = getInteger(props, RocketMQConfig.CONSUMER_BATCH_SIZE,
                RocketMQConfig.DEFAULT_CONSUMER_BATCH_SIZE);
        pullConsumerScheduleService.setPullThreadNums(pullPoolSize);
    }

    @Override
    public void run(SourceContext<OUT> context) {

        final Object lock = context.getCheckpointLock();

        pullConsumerScheduleService.registerPullTaskCallback(topic, new PullTaskCallback() {

            @Override
            public void doPullTask(MessageQueue mq, PullTaskContext pullTaskContext) {
                try {
                    long offset = getMessageQueueOffset(mq);
                    if (offset < 0) {
                        System.err.println("----------"+offsetTable);
                        return;
                    }
                    PullResult pullResult = consumer.pullBlockIfNotFound(mq, tag, offset, pullBatchSize);
                    putMessageQueueOffset(mq, offset,pullResult);
                    boolean found = false;
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            List<MessageExt> messages = pullResult.getMsgFoundList();
                            for (MessageExt msg : messages) {
                                byte[] key = msg.getKeys() != null ? msg.getKeys().getBytes(StandardCharsets.UTF_8) : null;
                                byte[] value = msg.getBody();
                                long bornTimestamp = msg.getBornTimestamp();

                                JSONObject map = new JSONObject();
                                Properties.convertToJSONObject(map,mq,offset,bornTimestamp);

                                OUT data = schema.deserializeKeyAndValue(key, value, map);

                                // output and state update are atomic
                                synchronized (lock) {
                                    context.collectWithTimestamp(data, bornTimestamp);
                                }
                            }
                            found = true;
                            break;
                        case NO_MATCHED_MSG:
                            LOG.debug("No matched message after offset {} for queue {}", offset, mq);
                            break;
                        case NO_NEW_MSG:
                            break;
                        case OFFSET_ILLEGAL:
                            LOG.warn("Offset {} is illegal for queue {}", offset, mq);
                            break;
                        default:
                            break;
                    }
                    if (found) {
                        pullTaskContext.setPullNextDelayTimeMillis(0); // no delay when messages were found
                    } else {
                        pullTaskContext.setPullNextDelayTimeMillis(delayWhenMessageNotFound);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            pullConsumerScheduleService.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }

        runningChecker.setRunning(true);
    }

    /**
     * 接口来提供类型信息提示
     * @return
     */
    @Override
    public TypeInformation<OUT> getProducedType() {
        return schema.getProducedType();
    }

    /**
     * 关闭程序
     */
    @Override
    public void cancel() {
        LOG.debug("cancel ...");
        runningChecker.setRunning(false);
        if (pullConsumerScheduleService != null) {
            pullConsumerScheduleService.shutdown();
        }
        offsetTable.clear();
    }

    /**
     * 获取offsetTable中保存的上一个状态
     * @param mq
     * @return
     * @throws MQClientException
     */
    private long getMessageQueueOffset(MessageQueue mq) throws MQClientException {
        Long offset;
        if (runOffset.equals(Boolean.FALSE)) {
            String initialOffset = props.getProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO, CONSUMER_OFFSET_LATEST);
            switch (initialOffset) {
                case CONSUMER_OFFSET_EARLIEST:
                    offset = consumer.minOffset(mq);
                    break;
                case CONSUMER_OFFSET_LATEST:
                    offset = consumer.maxOffset(mq);
                    break;
                case CONSUMER_OFFSET_TIMESTAMP:
                    offset = consumer.searchOffset(mq, getLong(props,
                            RocketMQConfig.CONSUMER_OFFSET_FROM_TIMESTAMP, System.currentTimeMillis()));
                    break;
                case CONSUMER_OFFSET_SITE:
                    String site = props.getProperty(RocketMQConfig.CONSUMER_OFFSET_SITE_STARTING_OFFSETS, DEFAULT_CONSUMER_OFFSET_SITE_STARTING_OFFSETS);
                    if (site.equals(DEFAULT_CONSUMER_OFFSET_SITE_STARTING_OFFSETS))
                        throw new RuntimeException("offset没有配置......");
                    else
                        offset = getMessageQueueStartingOffsets(mq, site);
                    break;
                default:
                    offset = consumer.fetchConsumeOffset(mq, false);
                    LOG.error("Unknown value for CONSUMER_OFFSET_RESET_TO.");
            }
            this.runOffset = Boolean.TRUE;
        }else if (runOffset.equals(Boolean.TRUE)) {
            offset = offsetTable.get(mq);
        }else {
            offset = consumer.fetchConsumeOffset(mq, false);
            LOG.error("Initialization failed......");
        }
        return offset;
    }

    private static final String EARLIEST = "-2";
    private static final String LATEST = "-1";
    /**
     * 获取外部保存的offset，可以实现指定位置启动
     * @param mq
     * @param site
     * @return
     */
    private Long getMessageQueueStartingOffsets(MessageQueue mq, String site) throws MQClientException {
        Map<MessageQueue, Long> map = getMessageQueueSite(site);
        Long offset = map.get(mq);
        if (offset.equals(LATEST))
            offset = consumer.maxOffset(mq);
        else if (offset.equals(EARLIEST))
            offset = consumer.minOffset(mq);
        else if (offset.equals(null) || offset == null){
            offset = consumer.minOffset(mq);
            LOG.error("没找到offset，该分区从最新位置启动......");
        } else
            return offset;
        return offset;
    }

    /**
     * 将外部配置好的offsetJSON转换成Map<MessageQueue, Long>
     * @param site
     * @return
     */
    private Map<MessageQueue, Long> getMessageQueueSite(String site) {
        Map<MessageQueue, Long> map = new ConcurrentHashMap<>();
        JSONObject siteJson = JSON.parseObject(site);
        for (Map.Entry<String, Object> siteMap : siteJson.entrySet()) {
            JSONObject broker = JSON.parseObject(siteMap.getValue().toString());
            for (Map.Entry<String, Object> brokerMap : broker.entrySet()) {
                JSONObject queue = JSON.parseObject(brokerMap.getValue().toString());
                for (Map.Entry<String, Object> queueMap : queue.entrySet()) {
                    MessageQueue mq = new MessageQueue();
                    mq.setTopic(siteMap.getKey());
                    mq.setBrokerName(brokerMap.getKey());
                    mq.setQueueId(Integer.parseInt(queueMap.getKey()));
                    map.put(mq,Long.parseLong(queueMap.getValue().toString()));
                }
            }
        }
        return map;
    }
    /**
     * 更新offsetTable中保存的最新状态
     * @param mq
     * @param offset
     * @throws MQClientException
     */
    private void putMessageQueueOffset(MessageQueue mq, long offset,PullResult pullResult) throws MQClientException {
        long offsets = pullResult.getNextBeginOffset();
        long tmpOffset = offset + 1;
        if (tmpOffset > offsets) offset = offsets;
        else offset = tmpOffset;
        offsetTable.put(mq, offset);
        consumer.updateConsumeOffset(mq, offset);
    }
}
