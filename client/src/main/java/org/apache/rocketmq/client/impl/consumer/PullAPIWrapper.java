/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.consumer.PopCallback;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class PullAPIWrapper {
    private final InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mQClientFactory;
    private final String consumerGroup;
    private final boolean unitMode;
    private ConcurrentMap<MessageQueue, AtomicLong/* brokerId */> pullFromWhichNodeTable =
        new ConcurrentHashMap<MessageQueue, AtomicLong>(32);
    private volatile boolean connectBrokerByUser = false;
    private volatile long defaultBrokerId = MixAll.MASTER_ID;
    private Random random = new Random(System.nanoTime());
    private ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    public PullAPIWrapper(MQClientInstance mQClientFactory, String consumerGroup, boolean unitMode) {
        this.mQClientFactory = mQClientFactory;
        this.consumerGroup = consumerGroup;
        this.unitMode = unitMode;
    }

    /**
     * 解码解压消息，并过滤消息
     *
     * @param mq
     * @param pullResult
     * @param subscriptionData
     * @return
     */
    public PullResult processPullResult(final MessageQueue mq, final PullResult pullResult,
        final SubscriptionData subscriptionData) {
        PullResultExt pullResultExt = (PullResultExt) pullResult;

        // 【1】设置该 mq 的下一次拉取消息的推荐服务器地址
        // 在消费者拉取消息时，broker 会根据情况 suggest 该消费者下一次到哪儿去拉消息（从服务器、主服务器）。
        // 消费者在 pullResult 中获取到该 suggest，并将建议的目标服务器设置到 pullFromWhichNodeTable。
        // 注意：当消息拉取进度过慢时，broker 可能会建议当前消费者去从服务器拉取消息。
        this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());

        if (PullStatus.FOUND == pullResult.getPullStatus()) {
            // 【2】响应结果解码解压。
            ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());
            List<MessageExt> msgList = MessageDecoder.decodesBatch(
                byteBuffer,
                this.mQClientFactory.getClientConfig().isDecodeReadBody(),
                this.mQClientFactory.getClientConfig().isDecodeDecompressBody(),
                true
            );

            // 【3】msgList里如果存在批量消息（即多个消息被压缩为一个消息），则需要将消息扁平化
            boolean needDecodeInnerMessage = false;
            for (MessageExt messageExt: msgList) {
                if (MessageSysFlag.check(messageExt.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG)
                    && MessageSysFlag.check(messageExt.getSysFlag(), MessageSysFlag.NEED_UNWRAP_FLAG)) {
                    needDecodeInnerMessage = true;
                    break;
                }
            }
            if (needDecodeInnerMessage) {
                List<MessageExt> innerMsgList = new ArrayList<MessageExt>();
                try {
                    for (MessageExt messageExt: msgList) {
                        if (MessageSysFlag.check(messageExt.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG)
                            && MessageSysFlag.check(messageExt.getSysFlag(), MessageSysFlag.NEED_UNWRAP_FLAG)) {
                            MessageDecoder.decodeMessage(messageExt, innerMsgList);
                        } else {
                            innerMsgList.add(messageExt);
                        }
                    }
                    msgList = innerMsgList;
                } catch (Throwable t) {
                    log.error("Try to decode the inner batch failed for {}", pullResult.toString(), t);
                }
            }

            // 【4】如果订阅信息中存在使用 tags 过滤的条件，切没有使用 Filter 过滤，则在此处进行过滤处理。
            List<MessageExt> msgListFilterAgain = msgList;
            if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) {
                msgListFilterAgain = new ArrayList<MessageExt>(msgList.size());
                for (MessageExt msg : msgList) {
                    if (msg.getTags() != null) {
                        if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                            msgListFilterAgain.add(msg);
                        }
                    }
                }
            }

            // 【5】调用程序员注册的 hook 进一步处理过滤出的消息
            if (this.hasHook()) {
                FilterMessageContext filterMessageContext = new FilterMessageContext();
                filterMessageContext.setUnitMode(unitMode);
                filterMessageContext.setMsgList(msgListFilterAgain);
                this.executeHook(filterMessageContext);
            }

            // 【6】对过滤出的消息进行最后一步的属性填充，比如：事务ID，本次拉取消息的最小偏移量，本次拉取消息的最大偏移量等。
            for (MessageExt msg : msgListFilterAgain) {
                String traFlag = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                if (Boolean.parseBoolean(traFlag)) {
                    msg.setTransactionId(msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                }
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET,
                    Long.toString(pullResult.getMinOffset()));
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET,
                    Long.toString(pullResult.getMaxOffset()));
                msg.setBrokerName(mq.getBrokerName());
                msg.setQueueId(mq.getQueueId());
                if (pullResultExt.getOffsetDelta() != null) {
                    msg.setQueueOffset(pullResultExt.getOffsetDelta() + msg.getQueueOffset());
                }
            }

            // 【7】将处理好的消息设置回 pullResultExt
            pullResultExt.setMsgFoundList(msgListFilterAgain);
        }

        // 【8】到这一步，由于拉取结果里的 二进制消息体 已经被解析成消息并设置到 pullResultExt 中。
        // 所以此时可以清空 pullResultExt 里的 二进制消息体。
        pullResultExt.setMessageBinary(null);

        return pullResult;
    }

    public void updatePullFromWhichNode(final MessageQueue mq, final long brokerId) {
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (null == suggest) {
            this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
        } else {
            suggest.set(brokerId);
        }
    }

    public boolean hasHook() {
        return !this.filterMessageHookList.isEmpty();
    }

    public void executeHook(final FilterMessageContext context) {
        if (!this.filterMessageHookList.isEmpty()) {
            for (FilterMessageHook hook : this.filterMessageHookList) {
                try {
                    hook.filterMessage(context);
                } catch (Throwable e) {
                    log.error("execute hook error. hookName={}", hook.hookName());
                }
            }
        }
    }

    public PullResult pullKernelImpl(
        final MessageQueue mq,
        final String subExpression,
        final String expressionType,
        final long subVersion,
        final long offset,
        final int maxNums,
        final int maxSizeInBytes,
        final int sysFlag,
        final long commitOffset,
        final long brokerSuspendMaxTimeMillis,
        final long timeoutMillis,
        final CommunicationMode communicationMode,
        final PullCallback pullCallback
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        // 从消费者本地缓存查找本次向 mq 拉取消息的请求应该发送到哪个 broker。
        FindBrokerResult findBrokerResult =
            this.mQClientFactory.findBrokerAddressInSubscribe(this.mQClientFactory.getBrokerNameFromMessageQueue(mq),
                this.recalculatePullFromWhichNode(mq), false);
        // 如果消费者本地缓存找不到本次拉取消息的目标 broker。
        // 则去 NameServer 拉取最新订阅信息并更新本地缓存，然后再次尝试查找目标 broker。
        if (null == findBrokerResult) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult =
                this.mQClientFactory.findBrokerAddressInSubscribe(this.mQClientFactory.getBrokerNameFromMessageQueue(mq),
                    this.recalculatePullFromWhichNode(mq), false);
        }

        // 如果还找不到请求发送的目标服务器，那么就报错。
        // 如果找到了目标服务器就执行请求的生成并发送。
        if (findBrokerResult != null) {
            {
                // check version
                if (!ExpressionType.isTagType(expressionType)
                    && findBrokerResult.getBrokerVersion() < MQVersion.Version.V4_1_0_SNAPSHOT.ordinal()) {
                    throw new MQClientException("The broker[" + mq.getBrokerName() + ", "
                        + findBrokerResult.getBrokerVersion() + "] does not upgrade to support for filter message by " + expressionType, null);
                }
            }
            int sysFlagInner = sysFlag;

            if (findBrokerResult.isSlave()) {
                sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
            }

            PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
            requestHeader.setConsumerGroup(this.consumerGroup);
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setQueueOffset(offset);
            requestHeader.setMaxMsgNums(maxNums);
            requestHeader.setSysFlag(sysFlagInner);
            requestHeader.setCommitOffset(commitOffset);
            requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
            requestHeader.setSubscription(subExpression);
            requestHeader.setSubVersion(subVersion);
            requestHeader.setMaxMsgBytes(maxSizeInBytes);
            requestHeader.setExpressionType(expressionType);
            requestHeader.setBname(mq.getBrokerName());

            String brokerAddr = findBrokerResult.getBrokerAddr();
            if (PullSysFlag.hasClassFilterFlag(sysFlagInner)) {
                brokerAddr = computePullFromWhichFilterServer(mq.getTopic(), brokerAddr);
            }

            PullResult pullResult = this.mQClientFactory.getMQClientAPIImpl().pullMessage(
                brokerAddr,
                requestHeader,
                timeoutMillis,
                communicationMode,
                pullCallback);

            return pullResult;
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    public PullResult pullKernelImpl(
        MessageQueue mq,
        final String subExpression,
        final String expressionType,
        final long subVersion,
        long offset,
        final int maxNums,
        final int sysFlag,
        long commitOffset,
        final long brokerSuspendMaxTimeMillis,
        final long timeoutMillis,
        final CommunicationMode communicationMode,
        PullCallback pullCallback
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return pullKernelImpl(
                mq,
                subExpression,
                expressionType,
                subVersion, offset,
                maxNums,
                Integer.MAX_VALUE,
                sysFlag,
                commitOffset,
                brokerSuspendMaxTimeMillis,
                timeoutMillis,
                communicationMode,
                pullCallback
        );
    }

    /**
     * 重新计算 queue 的本次拉取从哪个服务器节点拉去。
     *
     * @param mq
     * @return
     */
    public long recalculatePullFromWhichNode(final MessageQueue mq) {
        if (this.isConnectBrokerByUser()) {
            return this.defaultBrokerId;
        }

        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (suggest != null) {
            return suggest.get();
        }

        return MixAll.MASTER_ID;
    }

    private String computePullFromWhichFilterServer(final String topic, final String brokerAddr)
        throws MQClientException {
        ConcurrentMap<String, TopicRouteData> topicRouteTable = this.mQClientFactory.getTopicRouteTable();
        if (topicRouteTable != null) {
            TopicRouteData topicRouteData = topicRouteTable.get(topic);
            List<String> list = topicRouteData.getFilterServerTable().get(brokerAddr);

            if (list != null && !list.isEmpty()) {
                return list.get(randomNum() % list.size());
            }
        }

        throw new MQClientException("Find Filter Server Failed, Broker Addr: " + brokerAddr + " topic: "
            + topic, null);
    }

    public boolean isConnectBrokerByUser() {
        return connectBrokerByUser;
    }

    public void setConnectBrokerByUser(boolean connectBrokerByUser) {
        this.connectBrokerByUser = connectBrokerByUser;

    }

    public int randomNum() {
        int value = random.nextInt();
        if (value < 0) {
            value = Math.abs(value);
            if (value < 0)
                value = 0;
        }
        return value;
    }

    public void registerFilterMessageHook(ArrayList<FilterMessageHook> filterMessageHookList) {
        this.filterMessageHookList = filterMessageHookList;
    }

    public long getDefaultBrokerId() {
        return defaultBrokerId;
    }

    public void setDefaultBrokerId(long defaultBrokerId) {
        this.defaultBrokerId = defaultBrokerId;
    }


    /**
     *
     * @param mq
     * @param invisibleTime
     * @param maxNums
     * @param consumerGroup
     * @param timeout
     * @param popCallback
     * @param poll
     * @param initMode
    //     * @param expressionType
    //     * @param expression
     * @param order
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    public void popAsync(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup,
                         long timeout, PopCallback popCallback, boolean poll, int initMode, boolean order, String expressionType, String expression)
        throws MQClientException, RemotingException, InterruptedException {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (null == findBrokerResult) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        }
        if (findBrokerResult != null) {
            PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
            requestHeader.setConsumerGroup(consumerGroup);
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setMaxMsgNums(maxNums);
            requestHeader.setInvisibleTime(invisibleTime);
            requestHeader.setInitMode(initMode);
            requestHeader.setExpType(expressionType);
            requestHeader.setExp(expression);
            requestHeader.setOrder(order);
            //give 1000 ms for server response
            if (poll) {
                requestHeader.setPollTime(timeout);
                requestHeader.setBornTime(System.currentTimeMillis());
                // timeout + 10s, fix the too earlier timeout of client when long polling.
                timeout += 10 * 1000;
            }
            String brokerAddr = findBrokerResult.getBrokerAddr();
            this.mQClientFactory.getMQClientAPIImpl().popMessageAsync(mq.getBrokerName(), brokerAddr, requestHeader, timeout, popCallback);
            return;
        }
        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

}
