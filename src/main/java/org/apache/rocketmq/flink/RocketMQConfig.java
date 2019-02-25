/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.flink;

import java.util.Properties;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import static org.apache.rocketmq.flink.RocketMQUtils.getInteger;

/**
 * RocketMQConfig for Consumer/Producer.
 */
public class RocketMQConfig {
    // common
    public static final String NAME_SERVER_ADDR = "nameserver.address"; // Required

    public static final String NAME_SERVER_POLL_INTERVAL = "nameserver.poll.interval";
    public static final int DEFAULT_NAME_SERVER_POLL_INTERVAL = 1; // 30 seconds

    public static final String BROKER_HEART_BEAT_INTERVAL = "brokerserver.heartbeat.interval";
    public static final int DEFAULT_BROKER_HEART_BEAT_INTERVAL = 30000; // 30 seconds


    // producer
    public static final String PRODUCER_GROUP = "producer.group";

    public static final String PRODUCER_RETRY_TIMES = "producer.retry.times";
    public static final int DEFAULT_PRODUCER_RETRY_TIMES = 3;

    public static final String PRODUCER_TIMEOUT = "producer.timeout";
    public static final int DEFAULT_PRODUCER_TIMEOUT = 3000; // 3 seconds


    // consumer
    public static final String CONSUMER_GROUP = "consumer.group"; // Required

    public static final String CONSUMER_TOPIC = "consumer.topic"; // Required

    public static final String CONSUMER_TAG = "consumer.tag";
    public static final String DEFAULT_CONSUMER_TAG = "*";

    public static final String CONSUMER_OFFSET_RESET_TO = "consumer.offset.reset.to";
    public static final String CONSUMER_OFFSET_LATEST = "latest";
    public static final String CONSUMER_OFFSET_EARLIEST = "earliest";
    public static final String CONSUMER_OFFSET_TIMESTAMP = "timestamp";
    public static final String CONSUMER_OFFSET_SITE = "site";
    public static final String CONSUMER_OFFSET_ASSIGN = "assign";
    public static final String CONSUMER_OFFSET_FROM_TIMESTAMP = "consumer.offset.from.timestamp";
    public static final String CONSUMER_OFFSET_SITE_STARTING_OFFSETS = "starting.offsets";
    public static final String DEFAULT_CONSUMER_OFFSET_SITE_STARTING_OFFSETS = "try";

    public static final String CONSUMER_OFFSET_PERSIST_INTERVAL = "consumer.offset.persist.interval";
    public static final int DEFAULT_CONSUMER_OFFSET_PERSIST_INTERVAL = 5000; // 5 seconds

    public static final String CONSUMER_PULL_POOL_SIZE = "consumer.pull.thread.pool.size";
    public static final int DEFAULT_CONSUMER_PULL_POOL_SIZE = 20;

    public static final String CONSUMER_BATCH_SIZE = "consumer.batch.size";
    public static final int DEFAULT_CONSUMER_BATCH_SIZE = 32;

    public static final String CONSUMER_DELAY_WHEN_MESSAGE_NOT_FOUND = "consumer.delay.when.message.not.found";
    public static final int DEFAULT_CONSUMER_DELAY_WHEN_MESSAGE_NOT_FOUND = 10;

    /**
     * Build Producer Configs.
     * @param props Properties
     * @param producer DefaultMQProducer
     */
    public static void buildProducerConfigs(Properties props, DefaultMQProducer producer) {
        buildCommonConfigs(props, producer);

        String group = props.getProperty(PRODUCER_GROUP);
        if (StringUtils.isEmpty(group)) {
            group = UUID.randomUUID().toString();
        }
        producer.setProducerGroup(props.getProperty(PRODUCER_GROUP, group));

        producer.setRetryTimesWhenSendFailed(getInteger(props,
            PRODUCER_RETRY_TIMES, DEFAULT_PRODUCER_RETRY_TIMES));
        producer.setRetryTimesWhenSendAsyncFailed(getInteger(props,
            PRODUCER_RETRY_TIMES, DEFAULT_PRODUCER_RETRY_TIMES));
        producer.setSendMsgTimeout(getInteger(props,
            PRODUCER_TIMEOUT, DEFAULT_PRODUCER_TIMEOUT));
    }

    /**
     * Build Consumer Configs.
     * @param props Properties
     * @param consumer DefaultMQPushConsumer
     */
    public static void buildConsumerConfigs(Properties props, DefaultMQPullConsumer consumer) {
        buildCommonConfigs(props, consumer);

        consumer.setMessageModel(MessageModel.CLUSTERING);

        consumer.setPersistConsumerOffsetInterval(getInteger(props,
            CONSUMER_OFFSET_PERSIST_INTERVAL, DEFAULT_CONSUMER_OFFSET_PERSIST_INTERVAL));
    }

    /**
     * Build Common Configs.
     * @param props Properties
     * @param client ClientConfig
     */
    public static void buildCommonConfigs(Properties props, ClientConfig client) {
        String nameServers = props.getProperty(NAME_SERVER_ADDR);
        Validate.notEmpty(nameServers);
        client.setNamesrvAddr(nameServers);

        client.setPollNameServerInterval(getInteger(props,
            NAME_SERVER_POLL_INTERVAL, DEFAULT_NAME_SERVER_POLL_INTERVAL));
        client.setHeartbeatBrokerInterval(getInteger(props,
            BROKER_HEART_BEAT_INTERVAL, DEFAULT_BROKER_HEART_BEAT_INTERVAL));
    }
}
