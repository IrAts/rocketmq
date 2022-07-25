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
package org.apache.rocketmq.client.producer;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.hook.EndTransactionTraceHookImpl;
import org.apache.rocketmq.client.trace.hook.SendMessageTraceHookImpl;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * This class is the entry point for applications intending to send messages. </p>
 *
 * It's fine to tune fields which exposes getter/setter methods, but keep in mind, all of them should work well out of
 * box for most scenarios. </p>
 *
 * This class aggregates various <code>send</code> methods to deliver messages to broker(s). Each of them has pros and
 * cons; you'd better understand strengths and weakness of them before actually coding. </p>
 *
 * <p> <strong>Thread Safety:</strong> After configuring and starting process, this class can be regarded as thread-safe
 * and used among multiple threads context. </p>
 */
public class DefaultMQProducer extends ClientConfig implements MQProducer {

    /**
     * 生产者的内部默认实现，在构造生产者时内部自动初始化，提供了大部分方法的内部实现。
     */
    protected final transient DefaultMQProducerImpl defaultMQProducerImpl;
    private final InternalLogger log = ClientLogger.getLog();
    private final Set<Integer> retryResponseCodes = new CopyOnWriteArraySet<Integer>(Arrays.asList(
            ResponseCode.TOPIC_NOT_EXIST,
            ResponseCode.SERVICE_NOT_AVAILABLE,
            ResponseCode.SYSTEM_ERROR,
            ResponseCode.NO_PERMISSION,
            ResponseCode.NO_BUYER_ID,
            ResponseCode.NOT_IN_CURRENT_UNIT
    ));

    /**
     * 生产者的分组名称。相同的分组名称表明生产者实例在概念上归属于同一分组。
     * 这对事务消息十分重要，如果原始生产者在事务之后崩溃，那么broker可以联
     * 系同一生产者分组的不同生产者实例来提交或回滚事务。
     *
     * 默认值：DEFAULT_PRODUCER
     * 注意： 由数字、字母、下划线、横杠（-）、竖线（|）或百分号组成；不能为空；长度不能超过255。
     *
     * For non-transactional messages, it does not matter as long as it's unique per process. </p>
     *
     * See <a href="http://rocketmq.apache.org/docs/core-concept/">core concepts</a> for more discussion.
     */
    private String producerGroup;

    /**
     * 在发送消息时，自动创建服务器不存在的topic，需要指定Key，该Key可用于配置发送消息所在topic的默认路由。
     *
     * 建议：测试或者demo使用，生产环境下不建议打开自动创建配置。
     */
    private String createTopicKey = TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC;

    /**
     * 创建topic时默认的队列数量。
     */
    private volatile int defaultTopicQueueNums = 4;

    /**
     * 发送消息的超时时间。
     *
     * 建议：不建议修改该值，该值应该与broker配置中的sendTimeout一致，发送超时，可临时修改该值，建议解决超时问题，提高broker集群的Tps。
     */
    private int sendMsgTimeout = 3000;

    /**
     * 压缩消息体阈值。默认大于4K的消息体将默认进行压缩。
     *
     * 建议：
     * 可通过DefaultMQProducerImpl.setZipCompressLevel方法设置压缩率（默认为5，可选范围[0,9]）；
     * 可通过DefaultMQProducerImpl.tryToCompressMessage方法测试出compressLevel与compressMsgBodyOverHowmuch最佳值。
     */
    private int compressMsgBodyOverHowmuch = 1024 * 4;

    /**
     * 同步模式下，在返回发送失败之前，内部尝试重新发送消息的最大次数。
     *
     * 默认值：2，即：默认情况下一条消息最多会被投递3次。
     * 注意：在极端情况下，这可能会导致消息的重复。
     */
    private int retryTimesWhenSendFailed = 2;

    /**
     * 异步模式下，在发送失败之前，内部尝试重新发送消息的最大次数。
     *
     * 默认值：2，即：默认情况下一条消息最多会被投递3次。
     * 注意：在极端情况下，这可能会导致消息的重复。
     */
    private int retryTimesWhenSendAsyncFailed = 2;

    /**
     * 同步模式下，消息保存失败时是否重试其他broker。
     *
     * 默认值：false
     * 注意：此配置关闭时，非投递时产生异常情况下，会忽略retryTimesWhenSendFailed配置。
     */
    private boolean retryAnotherBrokerWhenNotStoreOK = false;

    /**
     * 消息体的最大大小。当消息体的字节数超过maxMessageSize就发送失败。
     *
     * 默认值：1024 * 1024 * 4，单位：字节
     */
    private int maxMessageSize = 1024 * 1024 * 4; // 4M

    /**
     * 基于RPCHooK实现的消息轨迹插件。在开启消息轨迹后，该类通过hook的方式把消息生产者，
     * 消息存储的broker和消费者消费消息的信息像链路一样记录下来。在构造生产者时根据构造
     * 入参enableMsgTrace来决定是否创建该对象。
     */
    private TraceDispatcher traceDispatcher = null;

    /**
     * 由默认参数值创建一个生产者。
     */
    public DefaultMQProducer() {
        this(null, MixAll.DEFAULT_PRODUCER_GROUP, null);
    }

    /**
     * 使用指定的hook创建一个生产者
     *
     * @param rpcHook 每个远程命令执行后会回调rpcHook
     */
    public DefaultMQProducer(RPCHook rpcHook) {
        this(null, MixAll.DEFAULT_PRODUCER_GROUP, rpcHook);
    }

    /**
     * 使用指定的分组名创建一个生产者
     *
     * @param producerGroup Producer group, see the name-sake field.
     */
    public DefaultMQProducer(final String producerGroup) {
        this(null, producerGroup, null);
    }

    /**
     * 使用指定的分组名及自定义hook创建一个生产者，并设置是否开启消息轨迹及追踪topic的名称。
     *
     * @param producerGroup 生产者的分组名称
     * @param rpcHook 每个远程命令执行后会回调rpcHook
     * @param enableMsgTrace 是否开启消息轨迹
     * @param customizedTraceTopic 消息轨迹topic的名称
     */
    public DefaultMQProducer(final String producerGroup, RPCHook rpcHook, boolean enableMsgTrace,
        final String customizedTraceTopic) {
        this(null, producerGroup, rpcHook, enableMsgTrace, customizedTraceTopic);
    }

    /**
     * Constructor specifying producer group.
     *
     * @param namespace Namespace for this MQ Producer instance.
     * @param producerGroup Producer group, see the name-sake field.
     */
    public DefaultMQProducer(final String namespace, final String producerGroup) {
        this(namespace, producerGroup, null);
    }

    /**
     * 使用指定的分组名及自定义hook创建一个生产者
     *
     * @param producerGroup 生产者的分组名称
     * @param rpcHook 每个远程命令执行后会回调rpcHook
     */
    public DefaultMQProducer(final String producerGroup, RPCHook rpcHook) {
        this(null, producerGroup, rpcHook);
    }

    /**
     * Constructor specifying namespace, producer group and RPC hook.
     *
     * @param namespace Namespace for this MQ Producer instance.
     * @param producerGroup Producer group, see the name-sake field.
     * @param rpcHook RPC hook to execute per each remoting command execution.
     */
    public DefaultMQProducer(final String namespace, final String producerGroup, RPCHook rpcHook) {
        this.namespace = namespace;
        this.producerGroup = producerGroup;
        defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
    }

    /**
     * 使用指定的分组名创建一个生产者，并设置是否开启消息轨迹。
     *
     * @param producerGroup 生产者的分组名称
     * @param enableMsgTrace 是否开启消息轨迹
     */
    public DefaultMQProducer(final String producerGroup, boolean enableMsgTrace) {
        this(null, producerGroup, null, enableMsgTrace, null);
    }

    /**
     * Constructor specifying producer group, enabled msgTrace flag and customized trace topic name.
     *
     * @param producerGroup 生产者的分组名称
     * @param enableMsgTrace 是否开启消息轨迹
     * @param customizedTraceTopic 消息轨迹topic的名称
     */
    public DefaultMQProducer(final String producerGroup, boolean enableMsgTrace, final String customizedTraceTopic) {
        this(null, producerGroup, null, enableMsgTrace, customizedTraceTopic);
    }

    /**
     * Constructor specifying namespace, producer group, RPC hook, enabled msgTrace flag and customized trace topic
     * name.
     *
     * @param namespace Namespace for this MQ Producer instance.
     * @param producerGroup 生产者的分组名称
     * @param rpcHook 每个远程命令执行后会回调rpcHook
     * @param enableMsgTrace 是否开启消息轨迹
     * @param customizedTraceTopic 消息轨迹topic的名称
     */
    public DefaultMQProducer(final String namespace, final String producerGroup, RPCHook rpcHook,
        boolean enableMsgTrace, final String customizedTraceTopic) {
        this.namespace = namespace;
        this.producerGroup = producerGroup;
        defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
        //if client open the message trace feature
        if (enableMsgTrace) {
            try {
                AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(producerGroup, TraceDispatcher.Type.PRODUCE, customizedTraceTopic, rpcHook);
                dispatcher.setHostProducer(this.defaultMQProducerImpl);
                traceDispatcher = dispatcher;
                this.defaultMQProducerImpl.registerSendMessageHook(
                    new SendMessageTraceHookImpl(traceDispatcher));
                this.defaultMQProducerImpl.registerEndTransactionHook(
                    new EndTransactionTraceHookImpl(traceDispatcher));
            } catch (Throwable e) {
                log.error("system mqtrace hook init failed ,maybe can't send msg trace data");
            }
        }
    }

    @Override
    public void setUseTLS(boolean useTLS) {
        super.setUseTLS(useTLS);
        if (traceDispatcher instanceof AsyncTraceDispatcher) {
            ((AsyncTraceDispatcher) traceDispatcher).getTraceProducer().setUseTLS(useTLS);
        }
    }
    
    /**
     * Start this producer instance. </p>
     *
     * <strong> Much internal initializing procedures are carried out to make this instance prepared, thus, it's a must
     * to invoke this method before sending or querying messages. </strong> </p>
     *
     * @throws MQClientException if there is any unexpected error.
     */
    @Override
    public void start() throws MQClientException {
        this.setProducerGroup(withNamespace(this.producerGroup));
        this.defaultMQProducerImpl.start();
        if (null != traceDispatcher) {
            try {
                traceDispatcher.start(this.getNamesrvAddr(), this.getAccessChannel());
            } catch (MQClientException e) {
                log.warn("trace dispatcher start failed ", e);
            }
        }
    }

    /**
     * This method shuts down this producer instance and releases related resources.
     */
    @Override
    public void shutdown() {
        this.defaultMQProducerImpl.shutdown();
        if (null != traceDispatcher) {
            traceDispatcher.shutdown();
        }
    }

    /**
     * 获取topic的消息队列。
     * Fetch message queues of topic <code>topic</code>, to which we may send/publish messages.
     *
     * @param topic topic名称
     * @return topic下的消息队列集合。
     * @throws MQClientException 生产者状态非Running；没有找到broker；broker返回失败；网络异常；线程中断等客户端异常。
     */
    @Override
    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        return this.defaultMQProducerImpl.fetchPublishMessageQueues(withNamespace(topic));
    }

    /**
     *
     * 以同步模式发送消息，仅当发送过程完全完成时，此方法才会返回。
     * 在返回发送失败之前，内部尝试重新发送消息的最大次数（参见retryTimesWhenSendFailed属性）。
     * 未明确指定发送队列，默认采取轮询策略发送。</p>
     *
     * <strong>Warn:</strong> this method has internal retry-mechanism, that is, internal implementation will retry
     * {@link #retryTimesWhenSendFailed} times before claiming failure. As a result, multiple messages may be potentially
     * delivered to broker(s). It's up to the application developers to resolve potential duplication issue.
     *
     * @param msg 待发送的消息。
     * @return {@link SendResult} 消息的发送结果，包含msgId，发送状态等信息。
     * @throws MQClientException broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。
     * @throws RemotingException  网络异常。
     * @throws MQBrokerException broker发生错误。
     * @throws InterruptedException 发送线程中断。
     */
    @Override
    public SendResult send(
        Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg);
    }

    /**
     * 以同步模式发送消息，如果在指定的超时时间内未完成消息投递，
     * 会抛出RemotingTooMuchRequestException。仅当发送过程完
     * 全完成时，此方法才会返回。 在返回发送失败之前，内部尝试重
     * 新发送消息的最大次数（参见retryTimesWhenSendFailed属性）。
     * 未明确指定发送队列，默认采取轮询策略发送。
     *
     * @param msg 	待发送的消息。
     * @param timeout 发送超时时间，单位：毫秒。
     * @return {@link SendResult} 消息的发送结果，包含msgId，发送状态等信息。
     * @throws MQClientException broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。
     * @throws RemotingException  网络异常。
     * @throws MQBrokerException broker发生错误。
     * @throws InterruptedException 发送线程中断。
     */
    @Override
    public SendResult send(Message msg,
        long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, timeout);
    }

    /**
     * 发送单条消息，异步发送调用后直接返回，并在在发送成功或者异常时回调sendCallback，
     * 所以异步发送时sendCallback参数不能为null，否则在回调时会抛出NullPointerException。
     * 异步发送时，在成功发送前，其内部会尝试重新发送消息的最大次数（参见retryTimesWhenSendAsyncFailed属性）。
     *
     * @param msg 待发送的消息。
     * @param sendCallback 回调接口的实现，在发送完成，处理成功和处理失败时回调。
     * @throws MQClientException broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。
     * @throws RemotingException  网络异常。
     * @throws InterruptedException 发送线程中断。
     */
    @Override
    public void send(Message msg,
        SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, sendCallback);
    }

    /**
     * 发送单条消息，异步发送调用后直接返回，并在在发送成功或者异常时回调sendCallback，
     * 所以异步发送时sendCallback参数不能为null，否则在回调时会抛出NullPointerException。
     * 若在指定时间内消息未发送成功，回调方法会收到RemotingTooMuchRequestException异常。
     * 异步发送时，在成功发送前，其内部会尝试重新发送消息的最大次数（参见retryTimesWhenSendAsyncFailed属性）。
     *
     * @param msg 待发送的消息。
     * @param sendCallback 回调接口的实现，在发送完成，处理成功和处理失败时回调。
     * @param timeout 发送超时时间，单位：毫秒。
     * @throws MQClientException broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。
     * @throws RemotingException  网络异常。
     * @throws InterruptedException 发送线程中断。
     */
    @Override
    public void send(Message msg, SendCallback sendCallback, long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, sendCallback, timeout);
    }

    /**
     * 以oneway形式发送消息，broker不会响应任何执行结果，和UDP类似。它具有最大的吞吐量但消息可能会丢失。
     * 可在消息量大，追求高吞吐量并允许消息丢失的情况下使用该方式。
     *
     * @param msg 待投递的消息
     * @throws MQClientException broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。
     * @throws RemotingException 网络异常。
     * @throws InterruptedException 发送线程中断。
     */
    @Override
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.sendOneway(msg);
    }

    /**
     * 向指定队列以oneway形式发送消息，broker不会响应任何执行结果，和UDP类似。它具有最大的吞吐量但消息可能会丢失。
     * 可在消息量大，追求高吞吐量并允许消息丢失的情况下使用该方式。
     *
     * @param msg 待投递的消息
     * @param mq 待投递的消息队列
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。
     * @throws RemotingException 网络异常。
     * @throws MQBrokerException broker发生错误。
     * @throws InterruptedException 发送线程中断。
     */
    @Override
    public SendResult send(Message msg, MessageQueue mq)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq));
    }

    /**
     * Same to {@link #send(Message)} with target message queue and send timeout specified.
     *
     * @param msg Message to send.
     * @param mq Target message queue.
     * @param timeout send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueue mq, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq), timeout);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with target message queue specified.
     *
     * @param msg Message to send.
     * @param mq Target message queue.
     * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq), sendCallback);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with target message queue and send timeout specified.
     *
     * @param msg Message to send.
     * @param mq Target message queue.
     * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
     * @param timeout Send timeout.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq), sendCallback, timeout);
    }

    /**
     * Same to {@link #sendOneway(Message)} with target message queue specified.
     *
     * @param msg Message to send.
     * @param mq Target message queue.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void sendOneway(Message msg,
        MessageQueue mq) throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.sendOneway(msg, queueWithNamespace(mq));
    }

    /**
     * Same to {@link #send(Message)} with message queue selector specified.
     *
     * @param msg Message to send.
     * @param selector Message queue selector, through which we get target message queue to deliver message to.
     * @param arg Argument to work along with message queue selector.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, selector, arg);
    }

    /**
     * Same to {@link #send(Message, MessageQueueSelector, Object)} with send timeout specified.
     *
     * @param msg Message to send.
     * @param selector Message queue selector, through which we get target message queue to deliver message to.
     * @param arg Argument to work along with message queue selector.
     * @param timeout Send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, selector, arg, timeout);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with message queue selector specified.
     *
     * @param msg 待发送的消息。
     * @param selector 队列选择器，用于选择发送的目标队列。
     * @param arg 供队列选择器使用的参数对象。
     * @param sendCallback 回调接口的实现。
     * @throws MQClientException broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。
     * @throws RemotingException 网络异常。
     * @throws InterruptedException 发送线程中断。
     */
    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback);
    }

    /**
     * 向通过MessageQueueSelector计算出的队列异步发送单条消息，异步发送调用后直接返回，
     * 在在发送成功或者异常时回调sendCallback，所以异步发送时sendCallback参数不能为null，
     * 否则在回调时会抛出NullPointerException。 异步发送时，在成功发送前，其内部会尝试重
     * 新发送消息的最大次数（参见retryTimesWhenSendAsyncFailed属性）。
     *
     * @param msg 待发送的消息。
     * @param selector 队列选择器，用于选择发送的目标队列。
     * @param arg 供队列选择器使用的参数对象。
     * @param sendCallback 回调接口的实现。
     * @param timeout 发送超时时间，单位：毫秒。
     * @throws MQClientException broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。
     * @throws RemotingException 网络异常。
     * @throws InterruptedException 发送线程中断。
     */
    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback, timeout);
    }

    /**
     * Send request message in synchronous mode. This method returns only when the consumer consume the request message and reply a message. </p>
     *
     * <strong>Warn:</strong> this method has internal retry-mechanism, that is, internal implementation will retry
     * {@link #retryTimesWhenSendFailed} times before claiming failure. As a result, multiple messages may be potentially
     * delivered to broker(s). It's up to the application developers to resolve potential duplication issue.
     *
     * @param msg request message to send
     * @param timeout request timeout
     * @return reply message
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any broker error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws RequestTimeoutException if request timeout.
     */
    @Override
    public Message request(final Message msg, final long timeout) throws RequestTimeoutException, MQClientException,
        RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.request(msg, timeout);
    }

    /**
     * Request asynchronously. </p>
     * This method returns immediately. On receiving reply message, <code>requestCallback</code> will be executed. </p>
     *
     * Similar to {@link #request(Message, long)}, internal implementation would potentially retry up to {@link
     * #retryTimesWhenSendAsyncFailed} times before claiming sending failure, which may yield message duplication and
     * application developers are the one to resolve this potential issue.
     *
     * @param msg request message to send
     * @param requestCallback callback to execute on request completion.
     * @param timeout request timeout
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws MQBrokerException if there is any broker error.
     */
    @Override
    public void request(final Message msg, final RequestCallback requestCallback, final long timeout)
        throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.request(msg, requestCallback, timeout);
    }

    /**
     * Same to {@link #request(Message, long)}  with message queue selector specified.
     *
     * @param msg request message to send
     * @param selector message queue selector, through which we get target message queue to deliver message to.
     * @param arg argument to work along with message queue selector.
     * @param timeout timeout of request.
     * @return reply message
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any broker error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws RequestTimeoutException if request timeout.
     */
    @Override
    public Message request(final Message msg, final MessageQueueSelector selector, final Object arg,
        final long timeout) throws MQClientException, RemotingException, MQBrokerException,
        InterruptedException, RequestTimeoutException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.request(msg, selector, arg, timeout);
    }

    /**
     * Same to {@link #request(Message, RequestCallback, long)} with target message selector specified.
     *
     * @param msg requst message to send
     * @param selector message queue selector, through which we get target message queue to deliver message to.
     * @param arg argument to work along with message queue selector.
     * @param requestCallback callback to execute on request completion.
     * @param timeout timeout of request.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws MQBrokerException if there is any broker error.
     */
    @Override
    public void request(final Message msg, final MessageQueueSelector selector, final Object arg,
        final RequestCallback requestCallback, final long timeout) throws MQClientException, RemotingException,
        InterruptedException, MQBrokerException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.request(msg, selector, arg, requestCallback, timeout);
    }

    /**
     * Same to {@link #request(Message, long)}  with target message queue specified in addition.
     *
     * @param msg request message to send
     * @param mq target message queue.
     * @param timeout request timeout
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any broker error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws RequestTimeoutException if request timeout.
     */
    @Override
    public Message request(final Message msg, final MessageQueue mq, final long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException, RequestTimeoutException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.request(msg, mq, timeout);
    }

    /**
     * Same to {@link #request(Message, RequestCallback, long)} with target message queue specified.
     *
     * @param msg request message to send
     * @param mq target message queue.
     * @param requestCallback callback to execute on request completion.
     * @param timeout timeout of request.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws MQBrokerException if there is any broker error.
     */
    @Override
    public void request(final Message msg, final MessageQueue mq, final RequestCallback requestCallback, long timeout)
        throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.request(msg, mq, requestCallback, timeout);
    }

    /**
     * Same to {@link #sendOneway(Message)} with message queue selector specified.
     *
     * @param msg Message to send.
     * @param selector Message queue selector, through which to determine target message queue to deliver message
     * @param arg Argument used along with message queue selector.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg)
        throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.sendOneway(msg, selector, arg);
    }

    /**
     * 发送事务消息。该类不做默认实现，抛出RuntimeException异常。参见：TransactionMQProducer类。
     *
     * @param msg 待投递的事务消息
     * @param tranExecuter 本地事务执行器。该类已过期，将在5.0.0版本中移除。请勿使用该方法。
     * @param arg 供本地事务执行程序使用的参数对象
     * @return 事务结果，参见：LocalTransactionState类。
     * @throws MQClientException if there is any client error.
     */
    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg, LocalTransactionExecuter tranExecuter,
        final Object arg)
        throws MQClientException {
        throw new RuntimeException("sendMessageInTransaction not implement, please use TransactionMQProducer class");
    }

    /**
     * 发送事务消息。该类不做默认实现，抛出RuntimeException异常。参见：TransactionMQProducer类。
     *
     * @param msg 待投递的事务消息
     * @param arg 供本地事务执行程序使用的参数对象
     * @return 事务结果，参见：LocalTransactionState类。
     * @throws MQClientException
     */
    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg,
        Object arg) throws MQClientException {
        throw new RuntimeException("sendMessageInTransaction not implement, please use TransactionMQProducer class");
    }

    /**
     * 在broker上创建一个topic。
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param key 访问密钥。
     * @param newTopic 新建topic的名称。由数字、字母、下划线（_）、横杠（-）、竖线（|）或百分号（%）组成；长度小于255；不能为TBW102或空。
     * @param queueNum topic的队列数量。
     * @param attributes
     * @throws MQClientException 生产者状态非Running；未找到broker等客户端异常。
     */
    @Deprecated
    @Override
    public void createTopic(String key, String newTopic, int queueNum, Map<String, String> attributes) throws MQClientException {
        createTopic(key, withNamespace(newTopic), queueNum, 0, null);
    }

    /**
     * 在broker上创建一个topic。
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param key 访问密钥。
     * @param newTopic 新建topic的名称。由数字、字母、下划线（_）、横杠（-）、竖线（|）或百分号（%）组成；长度小于255；不能为TBW102或空。
     * @param queueNum topic的队列数量。
     * @param topicSysFlag topic system flag
     * @param attributes
     * @throws MQClientException 生产者状态非Running；未找到broker等客户端异常。
     */
    @Deprecated
    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag, Map<String, String> attributes) throws MQClientException {
        this.defaultMQProducerImpl.createTopic(key, withNamespace(newTopic), queueNum, topicSysFlag);
    }

    /**
     * 查找指定时间的消息队列的物理偏移量。
     *
     * @param mq 要查询的消息队列。
     * @param timestamp 指定要查询时间的时间戳。单位：毫秒。从该时间戳往后查询。
     * @return 指定时间的消息队列的物理偏移量。
     * @throws MQClientException 生产者状态非Running；没有找到broker；broker返回失败；网络异常；线程中断等客户端异常。
     */
    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.defaultMQProducerImpl.searchOffset(queueWithNamespace(mq), timestamp);
    }

    /**
     * 查询给定消息队列的最大offset。
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param mq 要查询的消息队列
     * @return 给定消息队列的最大物理偏移量。
     * @throws MQClientException 生产者状态非Running；没有找到broker；broker返回失败；网络异常；线程中断等客户端异常。
     */
    @Deprecated
    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.maxOffset(queueWithNamespace(mq));
    }

    /**
     * 查询给定消息队列的最小offset。
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param mq 要查询的消息队列
     * @return 给定消息队列的最小物理偏移量。
     * @throws MQClientException 生产者状态非Running；没有找到broker；broker返回失败；网络异常；线程中断等客户端异常。
     */
    @Deprecated
    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.minOffset(queueWithNamespace(mq));
    }

    /**
     * 查询最早的消息存储时间。
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param mq 要查询的消息队列
     * @return 指定队列最早的消息存储时间。单位：毫秒。
     * @throws MQClientException 生产者状态非Running；没有找到broker；broker返回失败；网络异常；线程中断等客户端异常。
     */
    @Deprecated
    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.earliestMsgStoreTime(queueWithNamespace(mq));
    }

    /**
     * 根据给定的msgId查询消息。
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param offsetMsgId message id
     * @return Message specified.
     * @throws MQBrokerException if there is any broker error.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Deprecated
    @Override
    public MessageExt viewMessage(
        String offsetMsgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.defaultMQProducerImpl.viewMessage(offsetMsgId);
    }

    /**
     * 按关键字查询消息。
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param topic topic名称
     * @param key 查找的关键字
     * @param maxNum 返回消息的最大数量
     * @param begin 开始时间戳，单位：毫秒。从该时间开始拉取。
     * @param end 结束时间戳，单位：毫秒。拉取最大截止到该时间。
     * @return 查询到的消息集合。
     * @throws MQClientException 生产者状态非Running；没有找到broker；broker返回失败；网络异常等客户端异常客户端异常。
     * @throws InterruptedException 线程中断。
     */
    @Deprecated
    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        return this.defaultMQProducerImpl.queryMessage(withNamespace(topic), key, maxNum, begin, end);
    }

    /**
     * Query message of the given message ID.
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param topic Topic
     * @param msgId Message ID
     * @return Message specified.
     * @throws MQBrokerException if there is any broker error.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Deprecated
    @Override
    public MessageExt viewMessage(String topic,
        String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            return this.viewMessage(msgId);
        } catch (Exception ignored) {
        }
        return this.defaultMQProducerImpl.queryMessageByUniqKey(withNamespace(topic), msgId);
    }

    /**
     * 同步批量发送消息。在返回发送失败之前，内部尝试重新发送消息的最大次数（参见retryTimesWhenSendFailed属性）。未明确指定发送队列，默认采取轮询策略发送。
     *
     * @param msgs 待发送的消息集合。集合内的消息必须属同一个topic。
     * @return 批量消息的发送结果，包含msgId，发送状态等信息。
     * @throws MQClientException broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。
     * @throws RemotingException 网络异常。
     * @throws MQBrokerException broker发生错误。
     * @throws InterruptedException 发送线程中断。
     */
    @Override
    public SendResult send(
        Collection<Message> msgs) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs));
    }

    /**
     * 同步批量发送消息，如果在指定的超时时间内未完成消息投递，会抛出RemotingTooMuchRequestException。
     * 在返回发送失败之前，内部尝试重新发送消息的最大次数（参见retryTimesWhenSendFailed属性）。未明确指定发送队列，默认采取轮询策略发送。
     *
     * @param msgs 待发送的消息集合。集合内的消息必须属同一个topic。
     * @param timeout 发送超时时间，单位：毫秒。
     * @return 批量消息的发送结果，包含msgId，发送状态等信息。
     * @throws MQClientException broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。
     * @throws RemotingException 网络异常。
     * @throws MQBrokerException broker发生错误。
     * @throws InterruptedException 发送线程中断。
     */
    @Override
    public SendResult send(Collection<Message> msgs,
        long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), timeout);
    }

    /**
     * 向给定队列同步批量发送消息。
     *
     * 注意：指定队列意味着所有消息均为同一个topic。
     *
     * @param msgs 待发送的消息集合。集合内的消息必须属同一个topic。
     * @param messageQueue 待投递的消息队列。指定队列意味着待投递消息均为同一个topic。
     * @return 批量消息的发送结果，包含msgId，发送状态等信息。
     * @throws MQClientException broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。
     * @throws RemotingException 网络异常。
     * @throws MQBrokerException broker发生错误。
     * @throws InterruptedException 发送线程中断。
     */
    @Override
    public SendResult send(Collection<Message> msgs,
        MessageQueue messageQueue) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), messageQueue);
    }

    /**
     * 向给定队列同步批量发送消息，如果在指定的超时时间内未完成消息投递，会抛出RemotingTooMuchRequestException。
     *
     * 注意：指定队列意味着所有消息均为同一个topic。
     *
     * @param msgs 待发送的消息集合。集合内的消息必须属同一个topic。
     * @param messageQueue 待投递的消息队列。指定队列意味着待投递消息均为同一个topic。
     * @param timeout 发送超时时间，单位：毫秒。
     * @return 批量消息的发送结果，包含msgId，发送状态等信息。
     * @throws MQClientException broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。
     * @throws RemotingException 网络异常。
     * @throws MQBrokerException broker发生错误。
     * @throws InterruptedException 发送线程中断。
     */
    @Override
    public SendResult send(Collection<Message> msgs, MessageQueue messageQueue,
        long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), messageQueue, timeout);
    }

    @Override
    public void send(Collection<Message> msgs, SendCallback sendCallback) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQProducerImpl.send(batch(msgs), sendCallback);
    }

    @Override
    public void send(Collection<Message> msgs, SendCallback sendCallback,
        long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQProducerImpl.send(batch(msgs), sendCallback, timeout);
    }

    @Override
    public void send(Collection<Message> msgs, MessageQueue mq,
        SendCallback sendCallback) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQProducerImpl.send(batch(msgs), queueWithNamespace(mq), sendCallback);
    }

    @Override
    public void send(Collection<Message> msgs, MessageQueue mq,
        SendCallback sendCallback, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQProducerImpl.send(batch(msgs), queueWithNamespace(mq), sendCallback, timeout);
    }

    /**
     * Sets an Executor to be used for executing callback methods.
     *
     * @param callbackExecutor the instance of Executor
     */
    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.defaultMQProducerImpl.setCallbackExecutor(callbackExecutor);
    }

    /**
     * Sets an Executor to be used for executing asynchronous send.
     *
     * @param asyncSenderExecutor the instance of Executor
     */
    public void setAsyncSenderExecutor(final ExecutorService asyncSenderExecutor) {
        this.defaultMQProducerImpl.setAsyncSenderExecutor(asyncSenderExecutor);
    }

    /**
     * Add response code for retrying.
     *
     * @param responseCode response code, {@link ResponseCode}
     */
    public void addRetryResponseCode(int responseCode) {
        this.retryResponseCodes.add(responseCode);
    }

    private MessageBatch batch(Collection<Message> msgs) throws MQClientException {
        MessageBatch msgBatch;
        try {
            msgBatch = MessageBatch.generateFromList(msgs);
            for (Message message : msgBatch) {
                Validators.checkMessage(message, this);
                MessageClientIDSetter.setUniqID(message);
                message.setTopic(withNamespace(message.getTopic()));
            }
            MessageClientIDSetter.setUniqID(msgBatch);
            msgBatch.setBody(msgBatch.encode());
        } catch (Exception e) {
            throw new MQClientException("Failed to initiate the MessageBatch", e);
        }
        msgBatch.setTopic(withNamespace(msgBatch.getTopic()));
        return msgBatch;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getCreateTopicKey() {
        return createTopicKey;
    }

    public void setCreateTopicKey(String createTopicKey) {
        this.createTopicKey = createTopicKey;
    }

    public int getSendMsgTimeout() {
        return sendMsgTimeout;
    }

    public void setSendMsgTimeout(int sendMsgTimeout) {
        this.sendMsgTimeout = sendMsgTimeout;
    }

    public int getCompressMsgBodyOverHowmuch() {
        return compressMsgBodyOverHowmuch;
    }

    public void setCompressMsgBodyOverHowmuch(int compressMsgBodyOverHowmuch) {
        this.compressMsgBodyOverHowmuch = compressMsgBodyOverHowmuch;
    }

    @Deprecated
    public DefaultMQProducerImpl getDefaultMQProducerImpl() {
        return defaultMQProducerImpl;
    }

    public boolean isRetryAnotherBrokerWhenNotStoreOK() {
        return retryAnotherBrokerWhenNotStoreOK;
    }

    public void setRetryAnotherBrokerWhenNotStoreOK(boolean retryAnotherBrokerWhenNotStoreOK) {
        this.retryAnotherBrokerWhenNotStoreOK = retryAnotherBrokerWhenNotStoreOK;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public int getRetryTimesWhenSendFailed() {
        return retryTimesWhenSendFailed;
    }

    public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
        this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
    }

    public boolean isSendMessageWithVIPChannel() {
        return isVipChannelEnabled();
    }

    public void setSendMessageWithVIPChannel(final boolean sendMessageWithVIPChannel) {
        this.setVipChannelEnabled(sendMessageWithVIPChannel);
    }

    public long[] getNotAvailableDuration() {
        return this.defaultMQProducerImpl.getNotAvailableDuration();
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.defaultMQProducerImpl.setNotAvailableDuration(notAvailableDuration);
    }

    public long[] getLatencyMax() {
        return this.defaultMQProducerImpl.getLatencyMax();
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.defaultMQProducerImpl.setLatencyMax(latencyMax);
    }

    public boolean isSendLatencyFaultEnable() {
        return this.defaultMQProducerImpl.isSendLatencyFaultEnable();
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.defaultMQProducerImpl.setSendLatencyFaultEnable(sendLatencyFaultEnable);
    }

    public int getRetryTimesWhenSendAsyncFailed() {
        return retryTimesWhenSendAsyncFailed;
    }

    public void setRetryTimesWhenSendAsyncFailed(final int retryTimesWhenSendAsyncFailed) {
        this.retryTimesWhenSendAsyncFailed = retryTimesWhenSendAsyncFailed;
    }

    public TraceDispatcher getTraceDispatcher() {
        return traceDispatcher;
    }

    public Set<Integer> getRetryResponseCodes() {
        return retryResponseCodes;
    }
}
