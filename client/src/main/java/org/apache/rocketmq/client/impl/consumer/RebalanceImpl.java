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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueAssignment;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;

public abstract class RebalanceImpl {
    protected static final InternalLogger log = ClientLogger.getLog();

    protected final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>(64);
    protected final ConcurrentMap<MessageQueue, PopProcessQueue> popProcessQueueTable = new ConcurrentHashMap<MessageQueue, PopProcessQueue>(64);

    protected final ConcurrentMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable =
        new ConcurrentHashMap<String, Set<MessageQueue>>();
    protected final ConcurrentMap<String /* topic */, SubscriptionData> subscriptionInner =
        new ConcurrentHashMap<String, SubscriptionData>();
    protected String consumerGroup;
    protected MessageModel messageModel;
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;
    protected MQClientInstance mQClientFactory;
    private static final int TIMEOUT_CHECK_TIMES = 3;
    private static final int QUERY_ASSIGNMENT_TIMEOUT = 3000;

    private Map<String, String> topicBrokerRebalance = new ConcurrentHashMap<String, String>();
    private Map<String, String> topicClientRebalance = new ConcurrentHashMap<String, String>();

    public RebalanceImpl(String consumerGroup, MessageModel messageModel,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MQClientInstance mQClientFactory) {
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.mQClientFactory = mQClientFactory;
    }

    public void unlock(final MessageQueue mq, final boolean oneway) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(this.mQClientFactory.getBrokerNameFromMessageQueue(mq), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);
                log.warn("unlock messageQueue. group:{}, clientId:{}, mq:{}",
                    this.consumerGroup,
                    this.mQClientFactory.getClientId(),
                    mq);
            } catch (Exception e) {
                log.error("unlockBatchMQ exception, " + mq, e);
            }
        }
    }

    public void unlockAll(final boolean oneway) {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        for (final Map.Entry<String, Set<MessageQueue>> entry : brokerMqs.entrySet()) {
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty()) {
                continue;
            }

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);

                    for (MessageQueue mq : mqs) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            processQueue.setLocked(false);
                            log.info("the message queue unlock OK, Group: {} {}", this.consumerGroup, mq);
                        }
                    }
                } catch (Exception e) {
                    log.error("unlockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    private HashMap<String/* brokerName */, Set<MessageQueue>> buildProcessQueueTableByBrokerName() {
        HashMap<String, Set<MessageQueue>> result = new HashMap<String, Set<MessageQueue>>();

        for (Map.Entry<MessageQueue, ProcessQueue> entry : this.processQueueTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            ProcessQueue pq = entry.getValue();

            if (pq.isDropped()) {
                continue;
            }

            String destBrokerName = this.mQClientFactory.getBrokerNameFromMessageQueue(mq);
            Set<MessageQueue> mqs = result.get(destBrokerName);
            if (null == mqs) {
                mqs = new HashSet<MessageQueue>();
                result.put(mq.getBrokerName(), mqs);
            }

            mqs.add(mq);
        }

        return result;
    }

    public boolean lock(final MessageQueue mq) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(this.mQClientFactory.getBrokerNameFromMessageQueue(mq), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            LockBatchRequestBody requestBody = new LockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                Set<MessageQueue> lockedMq =
                    this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
                for (MessageQueue mmqq : lockedMq) {
                    ProcessQueue processQueue = this.processQueueTable.get(mmqq);
                    if (processQueue != null) {
                        processQueue.setLocked(true);
                        processQueue.setLastLockTimestamp(System.currentTimeMillis());
                    }
                }

                boolean lockOK = lockedMq.contains(mq);
                log.info("message queue lock {}, {} {}", lockOK ? "OK" : "Failed", this.consumerGroup, mq);
                return lockOK;
            } catch (Exception e) {
                log.error("lockBatchMQ exception, " + mq, e);
            }
        }

        return false;
    }

    public void lockAll() {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        Iterator<Entry<String, Set<MessageQueue>>> it = brokerMqs.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Set<MessageQueue>> entry = it.next();
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty()) {
                continue;
            }

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                LockBatchRequestBody requestBody = new LockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    Set<MessageQueue> lockOKMQSet =
                        this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);

                    for (MessageQueue mq : lockOKMQSet) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            if (!processQueue.isLocked()) {
                                log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                            }

                            processQueue.setLocked(true);
                            processQueue.setLastLockTimestamp(System.currentTimeMillis());
                        }
                    }
                    for (MessageQueue mq : mqs) {
                        if (!lockOKMQSet.contains(mq)) {
                            ProcessQueue processQueue = this.processQueueTable.get(mq);
                            if (processQueue != null) {
                                processQueue.setLocked(false);
                                log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("lockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    public boolean clientRebalance(String topic) {
        return true;
    }

    public boolean doRebalance(final boolean isOrder) {
        boolean balanced = true;
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                try {
                    if (!clientRebalance(topic) && tryQueryAssignment(topic)) {
                        balanced = this.getRebalanceResultFromBroker(topic, isOrder);
                    } else {
                        balanced = this.rebalanceByTopic(topic, isOrder);
                    }
                } catch (Throwable e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("rebalance Exception", e);
                        balanced = false;
                    }
                }
            }
        }

        this.truncateMessageQueueNotMyTopic();

        return balanced;
    }

    private boolean tryQueryAssignment(String topic) {
        if (topicClientRebalance.containsKey(topic)) {
            return false;
        }

        if (topicBrokerRebalance.containsKey(topic)) {
            return true;
        }
        String strategyName = allocateMessageQueueStrategy != null ? allocateMessageQueueStrategy.getName() : null;
        int retryTimes = 0;
        while (retryTimes++ < TIMEOUT_CHECK_TIMES) {
            try {
                Set<MessageQueueAssignment> resultSet = mQClientFactory.queryAssignment(topic, consumerGroup,
                    strategyName, messageModel, QUERY_ASSIGNMENT_TIMEOUT / TIMEOUT_CHECK_TIMES * retryTimes);
                topicBrokerRebalance.put(topic, topic);
                return true;
            } catch (Throwable t) {
                if (!(t instanceof RemotingTimeoutException)) {
                    log.error("tryQueryAssignment error.", t);
                    topicClientRebalance.put(topic, topic);
                    return false;
                }
            }
        }
        if (retryTimes >= TIMEOUT_CHECK_TIMES) {
            // if never success before and timeout exceed TIMEOUT_CHECK_TIMES, force client rebalance
            topicClientRebalance.put(topic, topic);
            return false;
        }
        return true;
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return subscriptionInner;
    }

    /**
     * 1、获取主题的队列，向 broker 发送请求，获取主题下，消费组所有消费者客户端ID。
     * 2、只有当 2 者均不为空时，才有必要进行 rebalance。
     * 3、在 rebalance 时，需要对 队列，还有消费者客户端 ID 进行排序，以确保同一个消费组下的视图是一致的。
     * 4、根据 分配策略 AllocateMessageQueueStrategy 为 消费者分配队列。
     *
     * @param topic
     * @param isOrder
     * @return
     */
    private boolean rebalanceByTopic(final String topic, final boolean isOrder) {
        boolean balanced = true;
        switch (messageModel) {
            case BROADCASTING: {
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                if (mqSet != null) {
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
                    if (changed) {
                        this.messageQueueChanged(topic, mqSet, mqSet);
                        log.info("messageQueueChanged {} {} {} {}", consumerGroup, topic, mqSet, mqSet);
                    }

                    balanced = mqSet.equals(getWorkingMessageQueue(topic));
                } else {
                    this.messageQueueChanged(topic, Collections.<MessageQueue>emptySet(), Collections.<MessageQueue>emptySet());
                    log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                }
                break;
            }
            case CLUSTERING: {
                // 获取主题的队列
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                // 向 broker 发送请求，获取该主题下，该消费组的所有 消费者客户端ID
                List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);
                if (null == mqSet) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        this.messageQueueChanged(topic, Collections.<MessageQueue>emptySet(), Collections.<MessageQueue>emptySet());
                        log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                    }
                }

                if (null == cidAll) {
                    log.warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
                }

                // topic 存在对列表并且存在消费者
                if (mqSet != null && cidAll != null) {
                    List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
                    mqAll.addAll(mqSet);

                    // 排序，同一消费组内，视图一致，确保同一个消费者队列不会被多个消费者分配
                    Collections.sort(mqAll);
                    Collections.sort(cidAll);

                    // 根据分配策略为消费者分配队列。
                    AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;
                    List<MessageQueue> allocateResult = null;
                    try {
                        allocateResult = strategy.allocate(
                            this.consumerGroup,
                            this.mQClientFactory.getClientId(),
                            mqAll,
                            cidAll);
                    } catch (Throwable e) {
                        log.error("allocate message queue exception. strategy name: {}, ex: {}", strategy.getName(), e);
                        return false;
                    }

                    Set<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
                    if (allocateResult != null) {
                        allocateResultSet.addAll(allocateResult);
                    }

                    // 更新消费者消息队列信息
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
                    if (changed) {
                        log.info(
                            "client rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, rebalanceResultSet={}",
                            strategy.getName(), consumerGroup, topic, this.mQClientFactory.getClientId(), mqSet.size(), cidAll.size(),
                            allocateResultSet.size(), allocateResultSet);
                        this.messageQueueChanged(topic, mqSet, allocateResultSet);
                    }

                    balanced = allocateResultSet.equals(getWorkingMessageQueue(topic));
                }
                break;
            }
            default:
                break;
        }

        return balanced;
    }

    private boolean getRebalanceResultFromBroker(final String topic, final boolean isOrder) {
        String strategyName = this.allocateMessageQueueStrategy.getName();
        Set<MessageQueueAssignment> messageQueueAssignments;
        try {
            messageQueueAssignments = this.mQClientFactory.queryAssignment(topic, consumerGroup,
                strategyName, messageModel, QUERY_ASSIGNMENT_TIMEOUT);
        } catch (Exception e) {
            log.error("allocate message queue exception. strategy name: {}, ex: {}", strategyName, e);
            return false;
        }

        // null means invalid result, we should skip the update logic
        if (messageQueueAssignments == null) {
            return false;
        }
        Set<MessageQueue> mqSet = new HashSet<MessageQueue>();
        for (MessageQueueAssignment messageQueueAssignment : messageQueueAssignments) {
            if (messageQueueAssignment.getMessageQueue() != null) {
                mqSet.add(messageQueueAssignment.getMessageQueue());
            }
        }
        Set<MessageQueue> mqAll = null;
        boolean changed = this.updateMessageQueueAssignment(topic, messageQueueAssignments, isOrder);
        if (changed) {
            log.info("broker rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, assignmentSet={}",
                strategyName, consumerGroup, topic, this.mQClientFactory.getClientId(), messageQueueAssignments);
            this.messageQueueChanged(topic, mqAll, mqSet);
        }

        return mqSet.equals(getWorkingMessageQueue(topic));
    }

    private Set<MessageQueue> getWorkingMessageQueue(String topic) {
        Set<MessageQueue> queueSet = new HashSet<MessageQueue>();
        for (Entry<MessageQueue, ProcessQueue> entry : this.processQueueTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            ProcessQueue pq = entry.getValue();

            if (mq.getTopic().equals(topic) && !pq.isDropped()) {
                queueSet.add(mq);
            }
        }

        for (Entry<MessageQueue, PopProcessQueue> entry : this.popProcessQueueTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            PopProcessQueue pq = entry.getValue();

            if (mq.getTopic().equals(topic) && !pq.isDropped()) {
                queueSet.add(mq);
            }
        }

        return queueSet;
    }

    private void truncateMessageQueueNotMyTopic() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();

        for (MessageQueue mq : this.processQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {

                ProcessQueue pq = this.processQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }

        for (MessageQueue mq : this.popProcessQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {

                PopProcessQueue pq = this.popProcessQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary pop mq, {}", consumerGroup, mq);
                }
            }
        }

        Iterator<Map.Entry<String, String>> clientIter = topicClientRebalance.entrySet().iterator();
        while (clientIter.hasNext()) {
            if (!subTable.containsKey(clientIter.next().getKey())) {
                clientIter.remove();
            }
        }

        Iterator<Map.Entry<String, String>> brokerIter = topicBrokerRebalance.entrySet().iterator();
        while (brokerIter.hasNext()) {
            if (!subTable.containsKey(brokerIter.next().getKey())) {
                brokerIter.remove();
            }
        }
    }

    /**
     * 更新消费者消息队列信息。
     * 如果 processQueue 中的 MessageQueue 不在刚分配的 MessageQueue 中，
     * 那么表示，该 MessageQueue 已分配给别的消费者，那么需要删除此 PullRequest
     *
     * @param topic
     * @param mqSet
     * @param isOrder
     * @return
     */
    private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet,
        final boolean isOrder) {
        boolean changed = false;

        Map<MessageQueue, MessageQueue> upgradeMqTable = new HashMap<MessageQueue, MessageQueue>();
        // 对 topic 重平衡后，需要把不再属于本消费者的 MessageQueue 移除，当然也需要移除 MessageQueue 在消费端的重现：ProcessQueue。
        // 在遍历 processQueueTable 移除 MessageQueue 时，仅仅是将 MessageQueue 从 processQueueTable 移除，并简单标记其 ProcessQueue 为 dropped。
        // 真正的移除还要讲 MessageQueue 对应的消费信息等清除，这个比较重量级的操作放到后面处理。
        // 因此，将需要被移除的 MessageQueue 和其对应的 ProcessQueue 记录在 removeQueueMap 中。
        HashMap<MessageQueue, ProcessQueue> removeQueueMap = new HashMap<MessageQueue, ProcessQueue>(this.processQueueTable.size());
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();
            if (mq.getTopic().equals(topic)) {
                if (!mqSet.contains(mq)) {
                    pq.setDropped(true);
                    removeQueueMap.put(mq, pq);
                } else if (pq.isPullExpired() && this.consumeType() == ConsumeType.CONSUME_PASSIVELY) {
                    pq.setDropped(true);
                    removeQueueMap.put(mq, pq);
                    log.error("[BUG]doRebalance, {}, try remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                        consumerGroup, mq);
                }
            }
        }
        // remove message queues no longer belong me
        // 执行移除 MessageQueue 后的清理动作，比如：清除消费进度，解除对顺序消费队列的锁定等。
        for (Entry<MessageQueue, ProcessQueue> entry : removeQueueMap.entrySet()) {
            MessageQueue mq = entry.getKey();
            ProcessQueue pq = entry.getValue();

            if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                this.processQueueTable.remove(mq);
                changed = true;
                log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
            }
        }


        // add new message queue
        boolean allMQLocked = true;
        List<PullRequest> pullRequestList = new ArrayList<PullRequest>();
        // 遍历分配到的所有 MessageQueue，如果 MessageQueue 本来就有本消费者消费，那么跳过。
        // 如果 MessageQueue 是新分配过来的，那么
        // 遍历刚分配的 MessageQueue，如果 processQueue 中不包含，则表示是刚添加的。
        for (MessageQueue mq : mqSet) {
            // 如果是新分配的 MessageQueue。
            if (!this.processQueueTable.containsKey(mq)) {
                // 如果是顺序消费那么尝试去 broker 锁定队列。
                if (isOrder && !this.lock(mq)) {
                    log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                    allMQLocked = false;
                    continue;
                }

                // 移除消费进度
                this.removeDirtyOffset(mq);
                // 创建新的 ProcessQueue 并将 Locked 标志设置为 true，表明 ProcessQueue 已经初始化。
                ProcessQueue pq = createProcessQueue(topic);
                pq.setLocked(true);
                // 计算下次从哪里拉取消息，计算下次拉取偏移量时，RocketMQ 提供了 3 种方式。
                // 可在消费者启动时，调用DefaultMQPushConsumer#setConsumeFromWhere 设置。
                // ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET：从队列当前最大偏移量开始消费
                // ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET 从最早可用的消息开始消费
                // ConsumeFromWhere.CONSUME_FROM_TIMESTAMP 从指定的时间戳开始消费
                long nextOffset = this.computePullFromWhere(mq);
                if (nextOffset >= 0) {
                    // 如果是顺序消费时，会尝试去 broker 锁定 MessageQueue（对应上方的 !this.lock(mq) 方法）。
                    // 如果锁定成功了，this.lock(mq) 方法会为 MessageQueue 创建 ProcessQueue 并存放到 processQueueTable 中（当然也会创建对应的拉取请求）。
                    // 也就是说，当顺序消费并且锁定成功时，此处的 putIfAbsent 不会做任何动作。
                    ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                    } else {
                        // 创建拉取请求并暂存起来，后续统一将拉取请求提交给工具人 PullMessageService。
                        log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                        PullRequest pullRequest = new PullRequest();
                        pullRequest.setConsumerGroup(consumerGroup);
                        pullRequest.setNextOffset(nextOffset);
                        pullRequest.setMessageQueue(mq);
                        pullRequest.setProcessQueue(pq);
                        pullRequestList.add(pullRequest);
                        changed = true;
                    }
                } else {
                    log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                }
            }

        }

        if (!allMQLocked) {
            mQClientFactory.rebalanceLater(500);
        }

        this.dispatchPullRequest(pullRequestList, 500);

        return changed;
    }

    private boolean updateMessageQueueAssignment(final String topic, final Set<MessageQueueAssignment> assignments,
        final boolean isOrder) {
        boolean changed = false;

        Map<MessageQueue, MessageQueueAssignment> mq2PushAssignment = new HashMap<MessageQueue, MessageQueueAssignment>();
        Map<MessageQueue, MessageQueueAssignment> mq2PopAssignment = new HashMap<MessageQueue, MessageQueueAssignment>();
        for (MessageQueueAssignment assignment : assignments) {
            MessageQueue messageQueue = assignment.getMessageQueue();
            if (messageQueue == null) {
                continue;
            }
            if (MessageRequestMode.POP == assignment.getMode()) {
                mq2PopAssignment.put(messageQueue, assignment);
            } else {
                mq2PushAssignment.put(messageQueue, assignment);
            }
        }

        if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            if (mq2PopAssignment.isEmpty() && !mq2PushAssignment.isEmpty()) {
                //pop switch to push
                //subscribe pop retry topic
                try {
                    final String retryTopic = KeyBuilder.buildPopRetryTopic(topic, getConsumerGroup());
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(retryTopic, SubscriptionData.SUB_ALL);
                    getSubscriptionInner().put(retryTopic, subscriptionData);
                } catch (Exception ignored) {
                }

            } else if (!mq2PopAssignment.isEmpty() && mq2PushAssignment.isEmpty()) {
                //push switch to pop
                //unsubscribe pop retry topic
                try {
                    final String retryTopic = KeyBuilder.buildPopRetryTopic(topic, getConsumerGroup());
                    getSubscriptionInner().remove(retryTopic);
                } catch (Exception ignored) {
                }

            }
        }

        {
            // drop process queues no longer belong me
            HashMap<MessageQueue, ProcessQueue> removeQueueMap = new HashMap<MessageQueue, ProcessQueue>(this.processQueueTable.size());
            Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<MessageQueue, ProcessQueue> next = it.next();
                MessageQueue mq = next.getKey();
                ProcessQueue pq = next.getValue();

                if (mq.getTopic().equals(topic)) {
                    if (!mq2PushAssignment.containsKey(mq)) {
                        pq.setDropped(true);
                        removeQueueMap.put(mq, pq);
                    } else if (pq.isPullExpired() && this.consumeType() == ConsumeType.CONSUME_PASSIVELY) {
                        pq.setDropped(true);
                        removeQueueMap.put(mq, pq);
                        log.error("[BUG]doRebalance, {}, try remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                            consumerGroup, mq);
                    }
                }
            }
            // remove message queues no longer belong me
            for (Entry<MessageQueue, ProcessQueue> entry : removeQueueMap.entrySet()) {
                MessageQueue mq = entry.getKey();
                ProcessQueue pq = entry.getValue();

                if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                    this.processQueueTable.remove(mq);
                    changed = true;
                    log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }

        {
            HashMap<MessageQueue, PopProcessQueue> removeQueueMap = new HashMap<MessageQueue, PopProcessQueue>(this.popProcessQueueTable.size());
            Iterator<Entry<MessageQueue, PopProcessQueue>> it = this.popProcessQueueTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<MessageQueue, PopProcessQueue> next = it.next();
                MessageQueue mq = next.getKey();
                PopProcessQueue pq = next.getValue();

                if (mq.getTopic().equals(topic)) {
                    if (!mq2PopAssignment.containsKey(mq)) {
                        //the queue is no longer your assignment
                        pq.setDropped(true);
                        removeQueueMap.put(mq, pq);
                    } else if (pq.isPullExpired() && this.consumeType() == ConsumeType.CONSUME_PASSIVELY) {
                        pq.setDropped(true);
                        removeQueueMap.put(mq, pq);
                        log.error("[BUG]doRebalance, {}, try remove unnecessary pop mq, {}, because pop is pause, so try to fixed it",
                            consumerGroup, mq);
                    }
                }
            }
            // remove message queues no longer belong me
            for (Entry<MessageQueue, PopProcessQueue> entry : removeQueueMap.entrySet()) {
                MessageQueue mq = entry.getKey();
                PopProcessQueue pq = entry.getValue();

                if (this.removeUnnecessaryPopMessageQueue(mq, pq)) {
                    this.popProcessQueueTable.remove(mq);
                    changed = true;
                    log.info("doRebalance, {}, remove unnecessary pop mq, {}", consumerGroup, mq);
                }
            }
        }

        {
            // add new message queue
            boolean allMQLocked = true;
            List<PullRequest> pullRequestList = new ArrayList<PullRequest>();
            for (MessageQueue mq : mq2PushAssignment.keySet()) {
                if (!this.processQueueTable.containsKey(mq)) {
                    if (isOrder && !this.lock(mq)) {
                        log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                        allMQLocked = false;
                        continue;
                    }

                    this.removeDirtyOffset(mq);
                    ProcessQueue pq = createProcessQueue();
                    pq.setLocked(true);
                    long nextOffset = -1L;
                    try {
                        nextOffset = this.computePullFromWhereWithException(mq);
                    } catch (Exception e) {
                        log.info("doRebalance, {}, compute offset failed, {}", consumerGroup, mq);
                        continue;
                    }

                    if (nextOffset >= 0) {
                        ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                        if (pre != null) {
                            log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                        } else {
                            log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                            PullRequest pullRequest = new PullRequest();
                            pullRequest.setConsumerGroup(consumerGroup);
                            pullRequest.setNextOffset(nextOffset);
                            pullRequest.setMessageQueue(mq);
                            pullRequest.setProcessQueue(pq);
                            pullRequestList.add(pullRequest);
                            changed = true;
                        }
                    } else {
                        log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                    }
                }
            }

            if (!allMQLocked) {
                mQClientFactory.rebalanceLater(500);
            }
            this.dispatchPullRequest(pullRequestList, 500);
        }

        {
            // add new message queue
            List<PopRequest> popRequestList = new ArrayList<PopRequest>();
            for (MessageQueue mq : mq2PopAssignment.keySet()) {
                if (!this.popProcessQueueTable.containsKey(mq)) {
                    PopProcessQueue pq = createPopProcessQueue();
                    PopProcessQueue pre = this.popProcessQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        log.info("doRebalance, {}, mq pop already exists, {}", consumerGroup, mq);
                    } else {
                        log.info("doRebalance, {}, add a new pop mq, {}", consumerGroup, mq);
                        PopRequest popRequest = new PopRequest();
                        popRequest.setTopic(topic);
                        popRequest.setConsumerGroup(consumerGroup);
                        popRequest.setMessageQueue(mq);
                        popRequest.setPopProcessQueue(pq);
                        popRequest.setInitMode(getConsumeInitMode());
                        popRequestList.add(popRequest);
                        changed = true;
                    }
                }
            }

            this.dispatchPopPullRequest(popRequestList, 500);
        }

        return changed;
    }

    public abstract void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll,
        final Set<MessageQueue> mqDivided);

    public abstract boolean removeUnnecessaryMessageQueue(final MessageQueue mq, final ProcessQueue pq);

    public boolean removeUnnecessaryPopMessageQueue(final MessageQueue mq, final PopProcessQueue pq) {
        return true;
    }

    public abstract ConsumeType consumeType();

    public abstract void removeDirtyOffset(final MessageQueue mq);

    /**
     * When the network is unstable, using this interface may return wrong offset.
     * It is recommended to use computePullFromWhereWithException instead.
     * @param mq
     * @return offset
     */
    @Deprecated
    public abstract long computePullFromWhere(final MessageQueue mq);

    public abstract long computePullFromWhereWithException(final MessageQueue mq) throws MQClientException;

    public abstract int getConsumeInitMode();

    public abstract void dispatchPullRequest(final List<PullRequest> pullRequestList, final long delay);

    public abstract void dispatchPopPullRequest(final List<PopRequest> pullRequestList, final long delay);

    public abstract ProcessQueue createProcessQueue();

    public abstract PopProcessQueue createPopProcessQueue();

    public abstract ProcessQueue createProcessQueue(String topicName);

    public void removeProcessQueue(final MessageQueue mq) {
        ProcessQueue prev = this.processQueueTable.remove(mq);
        if (prev != null) {
            boolean droped = prev.isDropped();
            prev.setDropped(true);
            this.removeUnnecessaryMessageQueue(mq, prev);
            log.info("Fix Offset, {}, remove unnecessary mq, {} Droped: {}", consumerGroup, mq, droped);
        }
    }

    public ConcurrentMap<MessageQueue, ProcessQueue> getProcessQueueTable() {
        return processQueueTable;
    }

    public ConcurrentMap<MessageQueue, PopProcessQueue> getPopProcessQueueTable() {
        return popProcessQueueTable;
    }

    public ConcurrentMap<String, Set<MessageQueue>> getTopicSubscribeInfoTable() {
        return topicSubscribeInfoTable;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public void destroy() {
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            next.getValue().setDropped(true);
        }

        this.processQueueTable.clear();

        Iterator<Entry<MessageQueue, PopProcessQueue>> popIt = this.popProcessQueueTable.entrySet().iterator();
        while (popIt.hasNext()) {
            Entry<MessageQueue, PopProcessQueue> next = popIt.next();
            next.getValue().setDropped(true);
        }
        this.popProcessQueueTable.clear();
    }

}
