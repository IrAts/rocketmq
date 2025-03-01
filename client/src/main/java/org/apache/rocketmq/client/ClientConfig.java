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
package org.apache.rocketmq.client;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.utils.NameServerAddressUtils;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RequestType;

/**
 * 客户端配置类，包含客户端的通用配置，如：
 *  客户端连接的 nameServer 的地址：namesrvAddr
 *  客户端IP：clientIP
 *  客户端实例名称：instanceName
 *  客户端执行回调的线程池线程数量：clientCallbackExecutorThreads
 *  客户端的命名空间：namespace
 *  客户端的命名空间是否被初始化：namespaceInitialized
 *  访问通道：accessChannel
 *
 *  从 nameServer 拉取 topic 信息的时间间隔
 *  向 broker 发送心跳帧的时间间隔
 *  消费者持久化偏移量的时间间隔
 *  当出现拉取信息异常时延迟拉取的时间间隔
 *  是否需要解码 message body
 *  是否需要解压 message body
 *  是否使用 TLS 协议
 *  客户端 API 超时时长
 */
public class ClientConfig {
    public static final String SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY = "com.rocketmq.sendMessageWithVIPChannel";
    /**
     * 配置 [decodeReadBody] 的 key
     */
    public static final String DECODE_READ_BODY = "com.rocketmq.read.body";
    /**
     * 配置 [decodeDecompressBody] 的 key
     */
    public static final String DECODE_DECOMPRESS_BODY = "com.rocketmq.decompress.body";
    private String namesrvAddr = NameServerAddressUtils.getNameServerAddresses();
    private String clientIP = RemotingUtil.getLocalAddress();
    private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    protected String namespace;
    private boolean namespaceInitialized = false;
    protected AccessChannel accessChannel = AccessChannel.LOCAL;

    /**
     * 从 nameServer 拉取 topic 信息的时间间隔（单位：毫秒）
     */
    private int pollNameServerInterval = 1000 * 30;
    /**
     * 向 broker 发送心跳帧的时间间隔（单位：毫秒）
     */
    private int heartbeatBrokerInterval = 1000 * 30;
    /**
     * 消费者持久化偏移量的时间间隔（单位：毫秒）
     */
    private int persistConsumerOffsetInterval = 1000 * 5;
    /**
     * 当出现拉取信息异常时延迟拉取的时间间隔（时间：毫秒）
     */
    private long pullTimeDelayMillsWhenException = 1000;
    private boolean unitMode = false;
    private String unitName;
    /**
     * 是否需要解码 message body
     */
    private boolean decodeReadBody = Boolean.parseBoolean(System.getProperty(DECODE_READ_BODY, "true"));
    /**
     * 是否需要解压 message body，仅当 decodeReadBody 为 true 时本参数才会被使用
     */
    private boolean decodeDecompressBody = Boolean.parseBoolean(System.getProperty(DECODE_DECOMPRESS_BODY, "true"));
    private boolean vipChannelEnabled = Boolean.parseBoolean(System.getProperty(SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, "false"));

    private boolean useTLS = TlsSystemConfig.tlsEnable;

    private int mqClientApiTimeout = 3 * 1000;

    private LanguageCode language = LanguageCode.JAVA;

    /**
     * Enable stream request type will inject a RPCHook to add corresponding request type to remoting layer.
     * And it will also generate a different client id to prevent unexpected reuses of MQClientInstance.
     */
    protected boolean enableStreamRequestType = false;

    public String buildMQClientId() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClientIP());

        sb.append("@");
        // {PID}#{nanoTime}
        sb.append(this.getInstanceName());
        if (!UtilAll.isBlank(this.unitName)) {
            sb.append("@");
            sb.append(this.unitName);
        }

        if (enableStreamRequestType) {
            sb.append("@");
            sb.append(RequestType.STREAM);
        }

        return sb.toString();
    }

    public String getClientIP() {
        return clientIP;
    }

    public void setClientIP(String clientIP) {
        this.clientIP = clientIP;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public void changeInstanceNameToPID() {
        if (this.instanceName.equals("DEFAULT")) {
            this.instanceName = UtilAll.getPid() + "#" + System.nanoTime();
        }
    }

    public String withNamespace(String resource) {
        return NamespaceUtil.wrapNamespace(this.getNamespace(), resource);
    }

    public Set<String> withNamespace(Set<String> resourceSet) {
        Set<String> resourceWithNamespace = new HashSet<String>();
        for (String resource : resourceSet) {
            resourceWithNamespace.add(withNamespace(resource));
        }
        return resourceWithNamespace;
    }

    public String withoutNamespace(String resource) {
        return NamespaceUtil.withoutNamespace(resource, this.getNamespace());
    }

    public Set<String> withoutNamespace(Set<String> resourceSet) {
        Set<String> resourceWithoutNamespace = new HashSet<String>();
        for (String resource : resourceSet) {
            resourceWithoutNamespace.add(withoutNamespace(resource));
        }
        return resourceWithoutNamespace;
    }

    public MessageQueue queueWithNamespace(MessageQueue queue) {
        if (StringUtils.isEmpty(this.getNamespace())) {
            return queue;
        }
        return new MessageQueue(withNamespace(queue.getTopic()), queue.getBrokerName(), queue.getQueueId());
    }

    public Collection<MessageQueue> queuesWithNamespace(Collection<MessageQueue> queues) {
        if (StringUtils.isEmpty(this.getNamespace())) {
            return queues;
        }
        Iterator<MessageQueue> iter = queues.iterator();
        while (iter.hasNext()) {
            MessageQueue queue = iter.next();
            queue.setTopic(withNamespace(queue.getTopic()));
        }
        return queues;
    }

    public void resetClientConfig(final ClientConfig cc) {
        this.namesrvAddr = cc.namesrvAddr;
        this.clientIP = cc.clientIP;
        this.instanceName = cc.instanceName;
        this.clientCallbackExecutorThreads = cc.clientCallbackExecutorThreads;
        this.pollNameServerInterval = cc.pollNameServerInterval;
        this.heartbeatBrokerInterval = cc.heartbeatBrokerInterval;
        this.persistConsumerOffsetInterval = cc.persistConsumerOffsetInterval;
        this.pullTimeDelayMillsWhenException = cc.pullTimeDelayMillsWhenException;
        this.unitMode = cc.unitMode;
        this.unitName = cc.unitName;
        this.vipChannelEnabled = cc.vipChannelEnabled;
        this.useTLS = cc.useTLS;
        this.namespace = cc.namespace;
        this.language = cc.language;
        this.mqClientApiTimeout = cc.mqClientApiTimeout;
        this.decodeReadBody = cc.decodeReadBody;
        this.decodeDecompressBody = cc.decodeDecompressBody;
        this.enableStreamRequestType = cc.enableStreamRequestType;
    }

    public ClientConfig cloneClientConfig() {
        ClientConfig cc = new ClientConfig();
        cc.namesrvAddr = namesrvAddr;
        cc.clientIP = clientIP;
        cc.instanceName = instanceName;
        cc.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
        cc.pollNameServerInterval = pollNameServerInterval;
        cc.heartbeatBrokerInterval = heartbeatBrokerInterval;
        cc.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
        cc.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
        cc.unitMode = unitMode;
        cc.unitName = unitName;
        cc.vipChannelEnabled = vipChannelEnabled;
        cc.useTLS = useTLS;
        cc.namespace = namespace;
        cc.language = language;
        cc.mqClientApiTimeout = mqClientApiTimeout;
        cc.decodeReadBody = decodeReadBody;
        cc.decodeDecompressBody = decodeDecompressBody;
        cc.enableStreamRequestType = enableStreamRequestType;
        return cc;
    }

    public String getNamesrvAddr() {
        if (StringUtils.isNotEmpty(namesrvAddr) && NameServerAddressUtils.NAMESRV_ENDPOINT_PATTERN.matcher(namesrvAddr.trim()).matches()) {
            return NameServerAddressUtils.getNameSrvAddrFromNamesrvEndpoint(namesrvAddr);
        }
        return namesrvAddr;
    }

    /**
     * Domain name mode access way does not support the delimiter(;), and only one domain name can be set.
     *
     * @param namesrvAddr name server address
     */
    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
        this.namespaceInitialized = false;
    }

    public int getClientCallbackExecutorThreads() {
        return clientCallbackExecutorThreads;
    }

    public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads) {
        this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
    }

    public int getPollNameServerInterval() {
        return pollNameServerInterval;
    }

    public void setPollNameServerInterval(int pollNameServerInterval) {
        this.pollNameServerInterval = pollNameServerInterval;
    }

    public int getHeartbeatBrokerInterval() {
        return heartbeatBrokerInterval;
    }

    public void setHeartbeatBrokerInterval(int heartbeatBrokerInterval) {
        this.heartbeatBrokerInterval = heartbeatBrokerInterval;
    }

    public int getPersistConsumerOffsetInterval() {
        return persistConsumerOffsetInterval;
    }

    public void setPersistConsumerOffsetInterval(int persistConsumerOffsetInterval) {
        this.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
    }

    public long getPullTimeDelayMillsWhenException() {
        return pullTimeDelayMillsWhenException;
    }

    public void setPullTimeDelayMillsWhenException(long pullTimeDelayMillsWhenException) {
        this.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean unitMode) {
        this.unitMode = unitMode;
    }

    public boolean isVipChannelEnabled() {
        return vipChannelEnabled;
    }

    public void setVipChannelEnabled(final boolean vipChannelEnabled) {
        this.vipChannelEnabled = vipChannelEnabled;
    }

    public boolean isUseTLS() {
        return useTLS;
    }

    public void setUseTLS(boolean useTLS) {
        this.useTLS = useTLS;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public boolean isDecodeReadBody() {
        return decodeReadBody;
    }

    public void setDecodeReadBody(boolean decodeReadBody) {
        this.decodeReadBody = decodeReadBody;
    }

    public boolean isDecodeDecompressBody() {
        return decodeDecompressBody;
    }

    public void setDecodeDecompressBody(boolean decodeDecompressBody) {
        this.decodeDecompressBody = decodeDecompressBody;
    }

    public String getNamespace() {
        if (namespaceInitialized) {
            return namespace;
        }

        if (StringUtils.isNotEmpty(namespace)) {
            return namespace;
        }

        if (StringUtils.isNotEmpty(this.namesrvAddr)) {
            if (NameServerAddressUtils.validateInstanceEndpoint(namesrvAddr)) {
                namespace = NameServerAddressUtils.parseInstanceIdFromEndpoint(namesrvAddr);
            }
        }
        namespaceInitialized = true;
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
        this.namespaceInitialized = true;
    }

    public AccessChannel getAccessChannel() {
        return this.accessChannel;
    }

    public void setAccessChannel(AccessChannel accessChannel) {
        this.accessChannel = accessChannel;
    }

    public int getMqClientApiTimeout() {
        return mqClientApiTimeout;
    }

    public void setMqClientApiTimeout(int mqClientApiTimeout) {
        this.mqClientApiTimeout = mqClientApiTimeout;
    }

    public boolean isEnableStreamRequestType() {
        return enableStreamRequestType;
    }

    public void setEnableStreamRequestType(boolean enableStreamRequestType) {
        this.enableStreamRequestType = enableStreamRequestType;
    }

    @Override
    public String toString() {
        return "ClientConfig [namesrvAddr=" + namesrvAddr + ", clientIP=" + clientIP + ", instanceName=" + instanceName
            + ", clientCallbackExecutorThreads=" + clientCallbackExecutorThreads + ", pollNameServerInterval=" + pollNameServerInterval
            + ", heartbeatBrokerInterval=" + heartbeatBrokerInterval + ", persistConsumerOffsetInterval=" + persistConsumerOffsetInterval
            + ", pullTimeDelayMillsWhenException=" + pullTimeDelayMillsWhenException + ", unitMode=" + unitMode + ", unitName=" + unitName + ", vipChannelEnabled="
            + vipChannelEnabled + ", useTLS=" + useTLS + ", language=" + language.name() + ", namespace=" + namespace + ", mqClientApiTimeout=" + mqClientApiTimeout
            + ", decodeReadBody=" + decodeReadBody + ", decodeDecompressBody=" + decodeDecompressBody
            + ", enableStreamRequestType=" + enableStreamRequestType + "]";
    }
}
