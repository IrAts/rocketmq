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
package org.apache.rocketmq.store;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.common.attribute.CQType;

/**
 * RocketMQ 存储路径为 ${ROCKETMQ_HOME}/store，主要存储文件如下：
 *  1、commitlog(dir) 消息存储目录
 *  2、config(dir) 运行期间的有一些配置信息，包含：
 *      consumerFilter.json 主题消息过滤信息
 *      consumerOffset.json 集群消费模式下的消息消费进度
 *      delayOffset.json 演示消息队列拉区进度
 *      subscriptionGroup.json 消息消费组的配置信息
 *      topics.json topic配置属性
 *  3、consumequeue(dir) 消息消费队列存储目录
 *  4、index(dir) 消息索引文件存储目录
 *  5、abort(file) 如果存在abort文件，说明Broker非正常关闭，该文件默认在启动Broker时创建，在正常退出前删除。
 *  6、checkpoint(file) 检查点文件，存储CommitLog文件最后一次刷盘时间戳、ConsumeQueue最后一次算盘时间、index文件最后一次刷盘时间戳。
 *
 * CommitLog 文件存储格式中前 4 字节存储该消息的总长度。
 * CommitLog 文件的默认存储目录：${ROCKETMQ_HOME}/store/commitlog，可通过在 broker 配置文件中设置 storePathRootDir 改变。
 * CommitLog 文件的默认大小为 1GB，可通过在 broker 配置文件中设置 mapedFileSizeCommitLog 来改变。
 *
 *
 * Store all metadata downtime for recovery, data protection reliability
 */
public class CommitLog implements Swappable {
    // Message's MAGIC CODE daa320a7
    public final static int MESSAGE_MAGIC_CODE = -626843481;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // End of file empty MAGIC CODE cbd43194
    public final static int BLANK_MAGIC_CODE = -875286124;
    protected final MappedFileQueue mappedFileQueue;
    protected final DefaultMessageStore defaultMessageStore;

    private final FlushManager flushManager;

    private final AppendMessageCallback appendMessageCallback;
    private final ThreadLocal<PutMessageThreadLocal> putMessageThreadLocal;

    protected volatile long confirmOffset = -1L;

    private volatile long beginTimeInLock = 0;

    protected final PutMessageLock putMessageLock;

    protected final TopicQueueLock topicQueueLock;

    private volatile Set<String> fullStorePaths = Collections.emptySet();

    private final FlushDiskWatcher flushDiskWatcher;

    protected int commitLogSize;

    public CommitLog(final DefaultMessageStore messageStore) {
        String storePath = messageStore.getMessageStoreConfig().getStorePathCommitLog();
        if (storePath.contains(MixAll.MULTI_PATH_SPLITTER)) {
            this.mappedFileQueue = new MultiPathMappedFileQueue(messageStore.getMessageStoreConfig(),
                messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(),
                messageStore.getAllocateMappedFileService(), this::getFullStorePaths);
        } else {
            this.mappedFileQueue = new MappedFileQueue(storePath,
                messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(),
                messageStore.getAllocateMappedFileService());
        }

        this.defaultMessageStore = messageStore;

        this.flushManager = new DefaultFlushManager();

        this.appendMessageCallback = new DefaultAppendMessageCallback();
        putMessageThreadLocal = new ThreadLocal<PutMessageThreadLocal>() {
            @Override
            protected PutMessageThreadLocal initialValue() {
                return new PutMessageThreadLocal(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
            }
        };
        this.putMessageLock = messageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();

        this.flushDiskWatcher = new FlushDiskWatcher();

        this.topicQueueLock = new TopicQueueLock();

        this.commitLogSize = messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
    }

    public void setFullStorePaths(Set<String> fullStorePaths) {
        this.fullStorePaths = fullStorePaths;
    }

    public Set<String> getFullStorePaths() {
        return fullStorePaths;
    }

    public long getTotalSize() {
        return this.mappedFileQueue.getTotalFileSize();
    }

    public ThreadLocal<PutMessageThreadLocal> getPutMessageThreadLocal() {
        return putMessageThreadLocal;
    }

    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        this.mappedFileQueue.checkSelf();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    public void start() {
        this.flushManager.start();
        log.info("start commitLog successfully. storeRoot: {}", this.defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
        flushDiskWatcher.setDaemon(true);
        flushDiskWatcher.start();
    }

    public void shutdown() {
        this.flushManager.shutdown();
        log.info("shutdown commitLog successfully. storeRoot: {}", this.defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
        flushDiskWatcher.shutdown(true);
    }

    public long flush() {
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedWhere();
    }

    public long getFlushedWhere() {
        return this.mappedFileQueue.getFlushedWhere();
    }

    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    public long remainHowManyDataToCommit() {
        return this.mappedFileQueue.remainHowManyDataToCommit();
    }

    public long remainHowManyDataToFlush() {
        return this.mappedFileQueue.remainHowManyDataToFlush();
    }

    public int deleteExpiredFile(
        final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately
    ) {
        return deleteExpiredFile(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately, 0);
    }

    public int deleteExpiredFile(
        final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately,
        final int deleteFileBatchMax
    ) {
        return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately, deleteFileBatchMax);
    }

    /**
     * Read CommitLog data, use data replication
     */
    public SelectMappedBufferResult getData(final long offset) {
        return this.getData(offset, offset == 0);
    }

    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos);
            return result;
        }

        return null;
    }

    public boolean getData(final long offset, final int size, final ByteBuffer byteBuffer) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.getData(pos, size, byteBuffer);
        }
        return false;
    }

    public List<SelectMappedBufferResult> getBulkData(final long offset, final int size) {
        List<SelectMappedBufferResult> bufferResultList = new ArrayList<>();

        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        int remainSize = size;
        long startOffset = offset;
        long maxOffset = this.getMaxOffset();
        if (offset + size > maxOffset) {
            remainSize = (int) (maxOffset - offset);
            log.warn("get bulk data size out of range, correct to max offset. offset: {}, size: {}, max: {}", offset, remainSize, maxOffset);
        }

        while (remainSize > 0) {
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(startOffset, startOffset == 0);
            if (mappedFile != null) {
                int pos = (int) (startOffset % mappedFileSize);
                int readableSize = mappedFile.getReadPosition() - pos;
                int readSize = Math.min(remainSize, readableSize);

                SelectMappedBufferResult bufferResult = mappedFile.selectMappedBuffer(pos, readSize);
                if (bufferResult == null) {
                    break;
                }
                bufferResultList.add(bufferResult);
                remainSize -= readSize;
                startOffset += readSize;
            }
        }

        return bufferResultList;
    }

    public SelectMappedFileResult getFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int size = (int) (mappedFile.getReadPosition() - offset % mappedFileSize);
            if (size > 0) {
                return new SelectMappedFileResult(size, mappedFile);
            }
        }
        return null;
    }

    //Create new mappedFile if not exits.
    public boolean getLastMappedFile(final long startOffset) {
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
        if (null == lastMappedFile) {
            log.error("getLastMappedFile error. offset:{}", startOffset);
            return false;
        }

        return true;
    }

    /**
     * Broker 正常退出时的文件恢复逻辑
     */
    public void recoverNormally(long maxPhyOffsetOfConsumeQueue) {
        // checkCRCOnRecover 参数用于在进行文件恢复时查找消息是否验证CRC码。
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        boolean checkDupInfo = this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable();
        // 1、Rroker正常停止再重启时，从倒数第3个文件开始恢复，如果不足3个文件，则从第一个文件开始恢复。
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Began to recover from the last third file
            int index = mappedFiles.size() - 3;
            if (index < 0) {
                index = 0;
            }


            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            // processOffset 为 CommitLog 文件已确认的物理偏移量。等于mappedFile.getFileFromOffset加上mappedFileOffset
            long processOffset = mappedFile.getFileFromOffset();
            // mappedFileOffset 为当前文件已校验通过的物理偏移量。
            long mappedFileOffset = 0;
            long lastValidMsgPhyOffset = this.getConfirmOffset();
            // normal recover doesn't require dispatching
            boolean doDispatch = false;
            // 2、遍历CommitLog文件，每次取出一条消息，如果查找结果为true并且消息的
            // 长度大于0，表示消息正确，mappedFileOffset指针向前移动本条消息的长度。
            // 如果查找结果为true并且消息的长度等于0，表示已到该文件的末尾，如果还有下
            // 一个文件，则重置processOffset、mappedFileOffset并重复上述步骤，否则
            // 跳出循环；如果查找结果为false，表明该文件未填满所有消息，则跳出循环，结
            // 束遍历文件。
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover, checkDupInfo);
                int size = dispatchRequest.getMsgSize();
                // Normal data
                if (dispatchRequest.isSuccess() && size > 0) {
                    lastValidMsgPhyOffset = processOffset + mappedFileOffset;
                    mappedFileOffset += size;
                    this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, false);
                }
                // Come the end of the file, switch to the next file Since the
                // return 0 representatives met last hole,
                // this can not be included in truncate offset
                else if (dispatchRequest.isSuccess() && size == 0) {
                    this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, true);
                    index++;
                    if (index >= mappedFiles.size()) {
                        // Current branch can not happen
                        log.info("recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
                // Intermediate file read error
                else if (!dispatchRequest.isSuccess()) {
                    if (size > 0) {
                        log.warn("found a half message at {}, it will be truncated.", processOffset + mappedFileOffset);
                    }
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
            }

            // 3、更新 MappedFileQueue 的 flushedWhere 和 committedPosition 指针。
            processOffset += mappedFileOffset;
            // Set a candidate confirm offset.
            // In most cases, this value will be overwritten by confirmLog.init.
            // It works if some confirmed messages are lost.
            this.setConfirmOffset(lastValidMsgPhyOffset);
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            // 4、删除processOffset之后的所有文件。遍历目录下的文件，如果文件的尾部偏移量小于offset则跳
            // 过该文件，如果尾部的偏移量大于offset，则进一步比较offset与文件的开始偏移量。如果offset大
            // 于文件的起始偏移量，说明当前文件包含了有效偏移量，设置MappedFile的flushedPosition和
            // committedPosition。如果offset小于文件的起始偏移量，说明该文件是有效文件后面创建的，则调用
            // MappedFile#destory方法释放MappedFile占用的内存资源（内存映射与内存通道等），然后加入待删
            // 除文件列表中，最终调用deleteExpiredFile将文件从物理磁盘上删除。
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            // Clear ConsumeQueue redundant data
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }

        } else {
            // Commitlog case files are deleted
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }
    }

    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
        final boolean checkDupInfo) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, checkDupInfo, true);
    }

    private void doNothingForDeadCode(final Object obj) {
        if (obj != null) {
            log.debug(String.valueOf(obj.hashCode()));
        }
    }

    /**
     * check the message and returns the message size
     *
     * @return 0 Come the end of the file // >0 Normal messages // -1 Message checksum failure
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
        final boolean checkDupInfo, final boolean readBody) {
        try {
            // 1 TOTAL SIZE
            int totalSize = byteBuffer.getInt();

            // 2 MAGIC CODE
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
                case MESSAGE_MAGIC_CODE:
                    break;
                case BLANK_MAGIC_CODE:
                    return new DispatchRequest(0, true /* success */);
                default:
                    log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                    return new DispatchRequest(-1, false /* success */);
            }

            byte[] bytesContent = new byte[totalSize];

            int bodyCRC = byteBuffer.getInt();

            int queueId = byteBuffer.getInt();

            int flag = byteBuffer.getInt();

            long queueOffset = byteBuffer.getLong();

            long physicOffset = byteBuffer.getLong();

            int sysFlag = byteBuffer.getInt();

            long bornTimeStamp = byteBuffer.getLong();

            ByteBuffer byteBuffer1;
            if ((sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0) {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }

            long storeTimestamp = byteBuffer.getLong();

            ByteBuffer byteBuffer2;
            if ((sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }

            int reconsumeTimes = byteBuffer.getInt();

            long preparedTransactionOffset = byteBuffer.getLong();

            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    byteBuffer.get(bytesContent, 0, bodyLen);

                    if (checkCRC) {
                        int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                        if (crc != bodyCRC) {
                            log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
                            return new DispatchRequest(-1, false/* success */);
                        }
                    }
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }

            byte topicLen = byteBuffer.get();
            byteBuffer.get(bytesContent, 0, topicLen);
            String topic = new String(bytesContent, 0, topicLen, MessageDecoder.CHARSET_UTF8);

            long tagsCode = 0;
            String keys = "";
            String uniqKey = null;

            short propertiesLength = byteBuffer.getShort();
            Map<String, String> propertiesMap = null;
            if (propertiesLength > 0) {
                byteBuffer.get(bytesContent, 0, propertiesLength);
                String properties = new String(bytesContent, 0, propertiesLength, MessageDecoder.CHARSET_UTF8);
                propertiesMap = MessageDecoder.string2messageProperties(properties);

                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);

                uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);

                if (checkDupInfo) {
                    String dupInfo = propertiesMap.get(MessageConst.DUP_INFO);
                    if (null == dupInfo || dupInfo.split("_").length != 2) {
                        log.warn("DupInfo in properties check failed. dupInfo={}", dupInfo);
                        return new DispatchRequest(-1, false);
                    }
                }

                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
                if (tags != null && tags.length() > 0) {
                    tagsCode = MessageExtBrokerInner.tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
                }

                // Timing message processing
                {
                    String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
                    if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(topic) && t != null) {
                        int delayLevel = Integer.parseInt(t);

                        if (delayLevel > this.defaultMessageStore.getMaxDelayLevel()) {
                            delayLevel = this.defaultMessageStore.getMaxDelayLevel();
                        }

                        if (delayLevel > 0) {
                            tagsCode = this.defaultMessageStore.computeDeliverTimestamp(delayLevel,
                                storeTimestamp);
                        }
                    }
                }
            }

            int readLength = calMsgLength(sysFlag, bodyLen, topicLen, propertiesLength);
            if (totalSize != readLength) {
                doNothingForDeadCode(reconsumeTimes);
                doNothingForDeadCode(flag);
                doNothingForDeadCode(bornTimeStamp);
                doNothingForDeadCode(byteBuffer1);
                doNothingForDeadCode(byteBuffer2);
                log.error(
                    "[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, bodyLen={}, topicLen={}, propertiesLength={}",
                    totalSize, readLength, bodyLen, topicLen, propertiesLength);
                return new DispatchRequest(totalSize, false/* success */);
            }

            DispatchRequest dispatchRequest = new DispatchRequest(
                topic,
                queueId,
                physicOffset,
                totalSize,
                tagsCode,
                storeTimestamp,
                queueOffset,
                keys,
                uniqKey,
                sysFlag,
                preparedTransactionOffset,
                propertiesMap
            );

            setBatchSizeIfNeeded(propertiesMap, dispatchRequest);

            return dispatchRequest;
        } catch (Exception e) {
        }

        return new DispatchRequest(-1, false /* success */);
    }

    private void setBatchSizeIfNeeded(Map<String, String> propertiesMap, DispatchRequest dispatchRequest) {
        if (null != propertiesMap && propertiesMap.containsKey(MessageConst.PROPERTY_INNER_NUM) && propertiesMap.containsKey(MessageConst.PROPERTY_INNER_BASE)) {
            dispatchRequest.setMsgBaseOffset(Long.parseLong(propertiesMap.get(MessageConst.PROPERTY_INNER_BASE)));
            dispatchRequest.setBatchSize(Short.parseShort(propertiesMap.get(MessageConst.PROPERTY_INNER_NUM)));
        }
    }

    protected static int calMsgLength(int sysFlag, int bodyLength, int topicLength, int propertiesLength) {
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int storehostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
        final int msgLen = 4 //TOTALSIZE 总长度
            + 4 //MAGICCODE 魔数
            + 4 //BODYCRC 消息体 crc 校验码
            + 4 //QUEUEID 消息消费队列ID
            + 4 //FLAG 消息标记，RocketMQ对其不作处理，供应用程序使用
            + 8 //QUEUEOFFSET 消息在 consumeQueue 的物理偏移量
            + 8 //PHYSICALOFFSET 消息在 commitLog 的物理偏移量
            + 4 //SYSFLAG 系统标志
            + 8 //BORNTIMESTAMP 消息生产者调用消息发送API时的时间戳
            + bornhostLength //BORNHOST 消息发送者IP
            + 8 //STORETIMESTAMP 消息存储时间戳
            + storehostAddressLength //STOREHOSTADDRESS broker服务器IP+端口号
            + 4 //RECONSUMETIMES 消息重试次数
            + 8 //Prepared Transaction Offset 事务消息的物理偏移量
            + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY 消息体长度
            + 1 + topicLength //TOPIC topic长度
            + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength 消息属性长度，2字节，即消息属性长度不能超过过65535
            + 0;
        return msgLen;
    }

    public long getConfirmOffset() {
        if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            return this.confirmOffset;
        } else {
            return getMaxOffset();
        }
    }

    public void setConfirmOffset(long phyOffset) {
        this.confirmOffset = phyOffset;
    }

    public long getLastFileFromOffset() {
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile();
        if (lastMappedFile != null) {
            if (lastMappedFile.isAvailable()) {
                return lastMappedFile.getFileFromOffset();
            }
        }

        return -1;
    }

    /**
     * Broker 异常停止恢复逻辑。
     * 异常文件恢复与正常停止文件恢复的步骤基本相同，主要差别有两个：首先，Broker正常停止默认从倒数
     * 第三个文件开始恢复，而异常停止则需要从最后一个文件倒序推进，找到第一个消息存储正常的文件；其
     * 次，如果CommitLog目录没有消息文件，在ConsuneQueue目录下存在的文件则需要销毁。
     *
     * @param maxPhyOffsetOfConsumeQueue
     */
    @Deprecated
    public void recoverAbnormally(long maxPhyOffsetOfConsumeQueue) {
        // recover by the minimum time stamp
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        boolean checkDupInfo = this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Looking beginning to recover from which file
            int index = mappedFiles.size() - 1;
            MappedFile mappedFile = null;
            for (; index >= 0; index--) {
                mappedFile = mappedFiles.get(index);
                // 判断一个消息文件是否正确。
                if (this.isMappedFileMatchedRecover(mappedFile)) {
                    // 如果找到MappedFile，则遍历MappedFile中的消息，验证消息的合法性，
                    // 并将消息重新转发到ConsumeQueue与Index文件。
                    // 如果未找到有效的MappedFile，则设置CommitLog目录的flushedWhere、
                    // committedWhere指针都为0，并销毁ConsumeQueue文件。
                    log.info("recover from this mapped file " + mappedFile.getFileName());
                    break;
                }
            }

            if (index < 0) {
                index = 0;
                mappedFile = mappedFiles.get(index);
            }

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            long lastValidMsgPhyOffset = this.getConfirmOffset();
            // abnormal recover require dispatching
            boolean doDispatch = true;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover, checkDupInfo);
                int size = dispatchRequest.getMsgSize();

                if (dispatchRequest.isSuccess()) {
                    // Normal data
                    if (size > 0) {
                        lastValidMsgPhyOffset = processOffset + mappedFileOffset;
                        mappedFileOffset += size;

                        if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                            if (dispatchRequest.getCommitLogOffset() < this.defaultMessageStore.getConfirmOffset()) {
                                this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, false);
                            }
                        } else {
                            this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, false);
                        }
                    }
                    // Come the end of the file, switch to the next file
                    // Since the return 0 representatives met last hole, this can
                    // not be included in truncate offset
                    else if (size == 0) {
                        this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, true);
                        index++;
                        if (index >= mappedFiles.size()) {
                            // The current branch under normal circumstances should
                            // not happen
                            log.info("recover physics file over, last mapped file " + mappedFile.getFileName());
                            break;
                        } else {
                            mappedFile = mappedFiles.get(index);
                            byteBuffer = mappedFile.sliceByteBuffer();
                            processOffset = mappedFile.getFileFromOffset();
                            mappedFileOffset = 0;
                            log.info("recover next physics file, " + mappedFile.getFileName());
                        }
                    }
                } else {

                    if (size > 0) {
                        log.warn("found a half message at {}, it will be truncated.", processOffset + mappedFileOffset);
                    }

                    log.info("recover physics file end, " + mappedFile.getFileName() + " pos=" + byteBuffer.position());
                    break;
                }
            }

            processOffset += mappedFileOffset;
            // Set a candidate confirm offset.
            // In most cases, this value will be overwritten by confirmLog.init.
            // It works if some confirmed messages are lost.
            this.setConfirmOffset(lastValidMsgPhyOffset);
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            // Clear ConsumeQueue redundant data
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }
        }
        // Commitlog case files are deleted
        else {
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }
    }

    public void truncateDirtyFiles(long phyOffset) {
        if (phyOffset <= this.getFlushedWhere()) {
            this.mappedFileQueue.setFlushedWhere(phyOffset);
        }

        if (phyOffset <= this.mappedFileQueue.getCommittedWhere()) {
            this.mappedFileQueue.setCommittedWhere(phyOffset);
        }

        this.mappedFileQueue.truncateDirtyFiles(phyOffset);
    }

    protected void onCommitLogAppend(MessageExtBrokerInner msg, AppendMessageResult result, MappedFile commitLogFile) {
        this.getMessageStore().onCommitLogAppend(msg, result, commitLogFile);
    }

    /**
     * 判断一个消息文件是否是正确的。
     */
    private boolean isMappedFileMatchedRecover(final MappedFile mappedFile) {
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

        // 1、判断文件的魔数，如果不是MESSAGE_MAGIC_CODE，则返回false，表示该文件不符合CommitLog文件的存储格式
        int magicCode = byteBuffer.getInt(MessageDecoder.MESSAGE_MAGIC_CODE_POSTION);
        if (magicCode != MESSAGE_MAGIC_CODE) {
            return false;
        }

        int sysFlag = byteBuffer.getInt(MessageDecoder.SYSFLAG_POSITION);
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        //2、如果文件中第一条消息的存储时间等于0，则返回false，说明该消息的存储文件中未存储任何消息
        int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
        long storeTimestamp = byteBuffer.getLong(msgStoreTimePos);
        if (0 == storeTimestamp) {
            return false;
        }

        // 3、对比文件第一条消息的时间戳与检测点。如果文件第一条消息的时间戳小于文件检测点，
        // 说明该文件的部分消息是可靠的，则从该文件开始恢复。checkpoint文件中保存了CommitLog、
        // ConsumeQueue、Index的文件刷盘点，RocketMQ默认选择CommitLog文件与ConsumeQueue
        // 这两个文件的刷盘点中较小值与CommitLog文件第一条消息的时间戳做对比，如果
        // messageIndexEnable为true，表示Index文件的刷盘时间点也参与计算。
        if (this.defaultMessageStore.getMessageStoreConfig().isMessageIndexEnable()
            && this.defaultMessageStore.getMessageStoreConfig().isMessageIndexSafe()) {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestampIndex()) {
                log.info("find check timestamp, {} {}",
                    storeTimestamp,
                    UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        } else {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestamp()) {
                log.info("find check timestamp, {} {}",
                    storeTimestamp,
                    UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        }

        return false;
    }

    public boolean resetOffset(long offset) {
        return this.mappedFileQueue.resetOffset(offset);
    }

    public long getBeginTimeInLock() {
        return beginTimeInLock;
    }

    public String generateKey(StringBuilder keyBuilder, MessageExt messageExt) {
        keyBuilder.setLength(0);
        keyBuilder.append(messageExt.getTopic());
        keyBuilder.append('-');
        keyBuilder.append(messageExt.getQueueId());
        return keyBuilder.toString();
    }

    public void setMappedFileQueueOffset(final long phyOffset) {
        this.mappedFileQueue.setFlushedWhere(phyOffset);
        this.mappedFileQueue.setCommittedWhere(phyOffset);
    }

    public void updateMaxMessageSize(PutMessageThreadLocal putMessageThreadLocal) {
        // dynamically adjust maxMessageSize, but not support DLedger mode temporarily
        int newMaxMessageSize = this.defaultMessageStore.getMessageStoreConfig().getMaxMessageSize();
        if (newMaxMessageSize >= 10 &&
                putMessageThreadLocal.getEncoder().getMaxMessageBodySize() != newMaxMessageSize) {
            putMessageThreadLocal.getEncoder().updateEncoderBufferCapacity(newMaxMessageSize);
        }
    }

    public CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
        // Set the storage time
        // 如果没有开启复制机制，那么将存储消息的时间设置为当前时刻
        if (!defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            msg.setStoreTimestamp(System.currentTimeMillis());
        }

        // Set the message body BODY CRC (consider the most appropriate setting
        // on the client)
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        // Back to Results
        AppendMessageResult result = null;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        String topic = msg.getTopic();

        // 根据消息的来源地址的IP协议版本设置标志位。
        InetSocketAddress bornSocketAddress = (InetSocketAddress) msg.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setBornHostV6Flag();
        }
        // 根据存储消息的broker的IP协议版本设置标志位
        InetSocketAddress storeSocketAddress = (InetSocketAddress) msg.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setStoreHostAddressV6Flag();
        }

        PutMessageThreadLocal putMessageThreadLocal = this.putMessageThreadLocal.get();
        updateMaxMessageSize(putMessageThreadLocal);
        // 生成key："topic-queueId"
        String topicQueueKey = generateKey(putMessageThreadLocal.getKeyBuilder(), msg);
        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        // 获取最后一个 Mapped File，也就是当前的最后一个 commitLog 文件
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        // 计算逻辑偏移量（一个commitLog文件1G，逻辑偏移量就是能大于1G的那个）
        long currOffset;
        if (mappedFile == null) {
            currOffset = 0;
        } else {
            currOffset = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }

        // 准备数据，如果需要进行多机数据冗余的话就会使用。
        int needAckNums = this.defaultMessageStore.getMessageStoreConfig().getInSyncReplicas();
        boolean needHandleHA = needHandleHA(msg);

        // 如果启动了高可用机制，那么判断可以同步当前消息的机器是否足够，不够的话返回存储消息失败。
        if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset) < this.defaultMessageStore.getMessageStoreConfig().getMinInSyncReplicas()) {
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
            if (this.defaultMessageStore.getMessageStoreConfig().isAllAckInSyncStateSet()) {
                // -1 means all ack in SyncStateSet
                needAckNums = MixAll.ALL_ACK_IN_SYNC_STATE_SET;
            }
        } else if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableSlaveActingMaster()) {
            int inSyncReplicas = Math.min(this.defaultMessageStore.getAliveReplicaNumInGroup(),
                this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset));
            needAckNums = calcNeedAckNums(inSyncReplicas);
            if (needAckNums > inSyncReplicas) {
                // Tell the producer, don't have enough slaves to handle the send request
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
        }

        // 以 "topic-queueId" 为粒度进行锁操作，尽可能提高并发。
        topicQueueLock.lock(topicQueueKey);
        try {

            // 为该消息在 consumeQueue 中分配存储的空间（20字节=8commitLog偏移量+4大小+tag哈希吗）并返回偏移量设置到 msg 中。
            // 注意是 consumeQueue 而不是 commitLog。
            // 当然当前 broker 是 slave 的话，那么就不需要执行分配操作。
            boolean needAssignOffset = true;
            if (defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()
                && defaultMessageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE) {
                needAssignOffset = false;
            }
            if (needAssignOffset) {
                // 在 consumeQueue 中分配存储的空间，并将偏移量设置到 msg 中。
                // 每个 Queue 的下一个分配偏移量都会在一个 Map topicQueueTable 记着，这需要使用key："topic-queueId" 就能获取到。
                defaultMessageStore.assignOffset(msg, getMessageNum(msg));
            }

            // 将 msg 转化为 ByteBuf 并存在 putMessageThreadLocal.getEncoder() 获取到的实体中。
            // 返回值如果不为空则表示转化失败。注意：此时 ByteBuf 中该消息存储的 commitLogOffset 无需设置。
            PutMessageResult encodeResult = putMessageThreadLocal.getEncoder().encode(msg);
            if (encodeResult != null) {
                return CompletableFuture.completedFuture(encodeResult);
            }
            // 这里将上面 msg 转化成的 ByteBuf 转换为 ByteBuffer 并拿出来，并设置到 setEncodedBuff 中。
            msg.setEncodedBuff(putMessageThreadLocal.getEncoder().getEncoderBuffer());
            PutMessageContext putMessageContext = new PutMessageContext(topicQueueKey);

            // 向 commitLog 写入消息前需要获取锁
            putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
            try {
                long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
                this.beginTimeInLock = beginLockTimestamp;

                // Here settings are stored timestamp, in order to ensure an orderly global
                if (!defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                    msg.setStoreTimestamp(beginLockTimestamp);
                }

                // mappedFile == null 代表 ${ROCKET_HOME}/store/commitlog 下不存在任何文件，也就是此时是第一次接收消息，需要创建新的。
                // mappedFile.isFull 表示当前文件满了，需要创建新的。
                if (null == mappedFile || mappedFile.isFull()) {
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
                }
                if (null == mappedFile) {
                    log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null));
                }

                // 存储消息到 mappedFile 中。
                result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
                switch (result.getStatus()) {
                    case PUT_OK:
                        onCommitLogAppend(msg, result, mappedFile);
                        break;
                    case END_OF_FILE:
                        onCommitLogAppend(msg, result, mappedFile);
                        unlockMappedFile = mappedFile;
                        // Create a new file, re-write the message
                        mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                        if (null == mappedFile) {
                            // XXX: warn and notify me
                            log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                            beginTimeInLock = 0;
                            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, result));
                        }
                        result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
                        if (AppendMessageStatus.PUT_OK.equals(result.getStatus())) {
                            onCommitLogAppend(msg, result, mappedFile);
                        }
                        break;
                    case MESSAGE_SIZE_EXCEEDED:
                    case PROPERTIES_SIZE_EXCEEDED:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                    case UNKNOWN_ERROR:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                    default:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                }

                // 计算锁定时间
                elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
                beginTimeInLock = 0;
            } finally {
                putMessageLock.unlock();
            }
        } finally {
            topicQueueLock.unlock(topicQueueKey);
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, msg.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).add(1);
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).add(result.getWroteBytes());

        return handleDiskFlushAndHA(putMessageResult, msg, needAckNums, needHandleHA);
    }

    public CompletableFuture<PutMessageResult> asyncPutMessages(final MessageExtBatch messageExtBatch) {
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());
        AppendMessageResult result;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        InetSocketAddress bornSocketAddress = (InetSocketAddress) messageExtBatch.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setBornHostV6Flag();
        }

        InetSocketAddress storeSocketAddress = (InetSocketAddress) messageExtBatch.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setStoreHostAddressV6Flag();
        }

        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        long currOffset;
        if (mappedFile == null) {
            currOffset = 0;
        } else {
            currOffset = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }

        int needAckNums = this.defaultMessageStore.getMessageStoreConfig().getInSyncReplicas();
        boolean needHandleHA = needHandleHA(messageExtBatch);


        if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset) < this.defaultMessageStore.getMessageStoreConfig().getMinInSyncReplicas()) {
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
            if (this.defaultMessageStore.getMessageStoreConfig().isAllAckInSyncStateSet()) {
                // -1 means all ack in SyncStateSet
                needAckNums = MixAll.ALL_ACK_IN_SYNC_STATE_SET;
            }
        } else if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableSlaveActingMaster()) {
            int inSyncReplicas = Math.min(this.defaultMessageStore.getAliveReplicaNumInGroup(),
                this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset));
            needAckNums = calcNeedAckNums(inSyncReplicas);
            if (needAckNums > inSyncReplicas) {
                // Tell the producer, don't have enough slaves to handle the send request
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
        }

        //fine-grained lock instead of the coarse-grained
        PutMessageThreadLocal pmThreadLocal = this.putMessageThreadLocal.get();
        updateMaxMessageSize(pmThreadLocal);
        MessageExtEncoder batchEncoder = pmThreadLocal.getEncoder();

        String topicQueueKey = generateKey(pmThreadLocal.getKeyBuilder(), messageExtBatch);

        PutMessageContext putMessageContext = new PutMessageContext(topicQueueKey);
        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch, putMessageContext));

        topicQueueLock.lock(topicQueueKey);
        try {
            defaultMessageStore.assignOffset(messageExtBatch, (short) putMessageContext.getBatchSize());

            putMessageLock.lock();
            try {
                long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
                this.beginTimeInLock = beginLockTimestamp;

                // Here settings are stored timestamp, in order to ensure an orderly
                // global
                messageExtBatch.setStoreTimestamp(beginLockTimestamp);

                if (null == mappedFile || mappedFile.isFull()) {
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
                }
                if (null == mappedFile) {
                    log.error("Create mapped file1 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null));
                }

                result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback, putMessageContext);
                switch (result.getStatus()) {
                    case PUT_OK:
                        break;
                    case END_OF_FILE:
                        unlockMappedFile = mappedFile;
                        // Create a new file, re-write the message
                        mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                        if (null == mappedFile) {
                            // XXX: warn and notify me
                            log.error("Create mapped file2 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                            beginTimeInLock = 0;
                            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, result));
                        }
                        result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback, putMessageContext);
                        break;
                    case MESSAGE_SIZE_EXCEEDED:
                    case PROPERTIES_SIZE_EXCEEDED:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                    case UNKNOWN_ERROR:
                    default:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                }

                elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
                beginTimeInLock = 0;
            } finally {
                putMessageLock.unlock();
            }
        } finally {
            topicQueueLock.unlock(topicQueueKey);
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessages in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, messageExtBatch.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).add(result.getMsgNum());
        storeStatsService.getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).add(result.getWroteBytes());

        return handleDiskFlushAndHA(putMessageResult, messageExtBatch, needAckNums, needHandleHA);
    }

    private int calcNeedAckNums(int inSyncReplicas) {
        int needAckNums = this.defaultMessageStore.getMessageStoreConfig().getInSyncReplicas();
        if (this.defaultMessageStore.getMessageStoreConfig().isEnableAutoInSyncReplicas()) {
            needAckNums = Math.min(needAckNums, inSyncReplicas);
            needAckNums = Math.max(needAckNums, this.defaultMessageStore.getMessageStoreConfig().getMinInSyncReplicas());
        }
        return needAckNums;
    }

    private boolean needHandleHA(MessageExt messageExt) {

        if (!messageExt.isWaitStoreMsgOK()) {
            /*
              No need to sync messages that special config to extra broker slaves.
              @see MessageConst.PROPERTY_WAIT_STORE_MSG_OK
             */
            return false;
        }

        if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            return false;
        }

        if (BrokerRole.SYNC_MASTER != this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {
            // No need to check ha in async or slave broker
            return false;
        }

        return true;
    }

    private CompletableFuture<PutMessageResult> handleDiskFlushAndHA(PutMessageResult putMessageResult,
        MessageExt messageExt, int needAckNums, boolean needHandleHA) {
        CompletableFuture<PutMessageStatus> flushResultFuture = handleDiskFlush(putMessageResult.getAppendMessageResult(), messageExt);
        CompletableFuture<PutMessageStatus> replicaResultFuture;
        if (!needHandleHA) {
            replicaResultFuture = CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        } else {
            replicaResultFuture = handleHA(putMessageResult.getAppendMessageResult(), putMessageResult, needAckNums);
        }

        return flushResultFuture.thenCombine(replicaResultFuture, (flushStatus, replicaStatus) -> {
            if (flushStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(flushStatus);
            }
            if (replicaStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(replicaStatus);
            }
            return putMessageResult;
        });
    }

    private CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, MessageExt messageExt) {
        return this.flushManager.handleDiskFlush(result, messageExt);
    }

    private CompletableFuture<PutMessageStatus> handleHA(AppendMessageResult result, PutMessageResult putMessageResult,
        int needAckNums) {
        if (needAckNums >= 0 && needAckNums <= 1) {
            return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        }

        HAService haService = this.defaultMessageStore.getHaService();

        long nextOffset = result.getWroteOffset() + result.getWroteBytes();

        // Wait enough acks from different slaves
        GroupCommitRequest request = new GroupCommitRequest(nextOffset, this.defaultMessageStore.getMessageStoreConfig().getSlaveTimeout(), needAckNums);
        haService.putRequest(request);
        haService.getWaitNotifyObject().wakeupAll();
        return request.future();
    }

    /**
     * According to receive certain message or offset storage time if an error occurs, it returns -1
     */
    public long pickupStoreTimestamp(final long offset, final int size) {
        if (offset >= this.getMinOffset()) {
            SelectMappedBufferResult result = this.getMessage(offset, size);
            if (null != result) {
                try {
                    int sysFlag = result.getByteBuffer().getInt(MessageDecoder.SYSFLAG_POSITION);
                    int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
                    int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
                    return result.getByteBuffer().getLong(msgStoreTimePos);
                } finally {
                    result.release();
                }
            }
        }

        return -1;
    }

    /**
     * 获取当前 commitLog 目录的最小偏移量，首先获取目录下的第一个文件。
     * 如果该文件可用，则返回该文件的起始偏移量。
     * 否则返回下一个文件的起始偏移量。
     */
    public long getMinOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            if (mappedFile.isAvailable()) {
                return mappedFile.getFileFromOffset();
            } else {
                return this.rollNextFile(mappedFile.getFileFromOffset());
            }
        }

        return -1;
    }

    /**
     * 根据偏移量与消息长度查找消息。首先根据偏移找到文件所在的物理偏移量，
     * 然后用 offset 与文件长度取余，得到在文件内的偏移量，从该偏移量读取
     * size 长度的内容并返回。如果只根据消息偏移量查找消息，则首先找找到文
     * 件内的偏移量，然后尝试读取4字节，获取消息的实际长度，最后读取指定字节。
     *
     * @param offset
     * @param size
     * @return
     */
    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }

    /**
     * 根据 offset 返回下一个文件的起始偏移量。
     * offset + mappedFileSize - offset % mappedFileSize;
     */
    public long rollNextFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    public boolean appendData(long startOffset, byte[] data, int dataStart, int dataLength) {
        putMessageLock.lock();
        try {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
            if (null == mappedFile) {
                log.error("appendData getLastMappedFile error  " + startOffset);
                return false;
            }

            return mappedFile.appendMessage(data, dataStart, dataLength);
        } finally {
            putMessageLock.unlock();
        }
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        return this.mappedFileQueue.retryDeleteFirstFile(intervalForcibly);
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
    }

    public long lockTimeMills() {
        long diff = 0;
        long begin = this.beginTimeInLock;
        if (begin > 0) {
            diff = this.defaultMessageStore.now() - begin;
        }

        if (diff < 0) {
            diff = 0;
        }

        return diff;
    }

    protected short getMessageNum(MessageExtBrokerInner msgInner) {
        short messageNum = 1;
        // IF inner batch, build batchQueueOffset and batchNum property.
        CQType cqType = getCqType(msgInner);

        if (MessageSysFlag.check(msgInner.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG) || CQType.BatchCQ.equals(cqType)) {
            if (msgInner.getProperty(MessageConst.PROPERTY_INNER_NUM) != null) {
                messageNum = Short.parseShort(msgInner.getProperty(MessageConst.PROPERTY_INNER_NUM));
                messageNum = messageNum >= 1 ? messageNum : 1;
            }
        }

        return messageNum;
    }

    private CQType getCqType(MessageExtBrokerInner msgInner) {
        Optional<TopicConfig> topicConfig = this.defaultMessageStore.getTopicConfig(msgInner.getTopic());
        return QueueTypeUtils.getCQType(topicConfig);
    }

    abstract class FlushCommitLogService extends ServiceThread {
        protected static final int RETRY_TIMES_OVER = 10;
    }

    class CommitRealTimeService extends FlushCommitLogService {

        private long lastCommitTimestamp = 0;

        @Override
        public String getServiceName() {
            if (CommitLog.this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return CommitLog.this.defaultMessageStore.getBrokerIdentity().getLoggerIdentifier() + CommitRealTimeService.class.getSimpleName();
            }
            return CommitRealTimeService.class.getSimpleName();
        }

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();

                int commitDataLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogLeastPages();

                int commitDataThoroughInterval =
                    CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogThoroughInterval();

                long begin = System.currentTimeMillis();
                if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                    this.lastCommitTimestamp = begin;
                    commitDataLeastPages = 0;
                }

                try {
                    boolean result = CommitLog.this.mappedFileQueue.commit(commitDataLeastPages);
                    long end = System.currentTimeMillis();
                    if (!result) {
                        this.lastCommitTimestamp = end; // result = false means some data committed.
                        CommitLog.this.flushManager.wakeUpFlush();
                    }
                    CommitLog.this.getMessageStore().getPerfCounter().flowOnce("COMMIT_DATA_TIME_MS", (int) (end - begin));
                    if (end - begin > 500) {
                        log.info("Commit data to file costs {} ms", end - begin);
                    }
                    this.waitForRunning(interval);
                } catch (Throwable e) {
                    CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
                }
            }

            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.commit(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }
            CommitLog.log.info(this.getServiceName() + " service end");
        }
    }

    class FlushRealTimeService extends FlushCommitLogService {
        private long lastFlushTimestamp = 0;
        private long printTimes = 0;

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                boolean flushCommitLogTimed = CommitLog.this.defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();

                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();
                int flushPhysicQueueLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();

                int flushPhysicQueueThoroughInterval =
                    CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

                boolean printFlushProgress = false;

                // Print flush progress
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    flushPhysicQueueLeastPages = 0;
                    printFlushProgress = (printTimes++ % 10) == 0;
                }

                try {
                    if (flushCommitLogTimed) {
                        Thread.sleep(interval);
                    } else {
                        this.waitForRunning(interval);
                    }

                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }

                    long begin = System.currentTimeMillis();
                    CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }
                    long past = System.currentTimeMillis() - begin;
                    CommitLog.this.getMessageStore().getPerfCounter().flowOnce("FLUSH_DATA_TIME_MS", (int) past);
                    if (past > 500) {
                        log.info("Flush data to disk costs {} ms", past);
                    }
                } catch (Throwable e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                    this.printFlushProgress();
                }
            }

            // Normal shutdown, to ensure that all the flush before exit
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.flush(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }

            this.printFlushProgress();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            if (CommitLog.this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return CommitLog.this.defaultMessageStore.getBrokerConfig().getLoggerIdentifier() + FlushRealTimeService.class.getSimpleName();
            }
            return FlushRealTimeService.class.getSimpleName();
        }

        private void printFlushProgress() {
            // CommitLog.log.info("how much disk fall behind memory, "
            // + CommitLog.this.mappedFileQueue.howMuchFallBehind());
        }

        @Override
        public long getJoinTime() {
            return 1000 * 60 * 5;
        }
    }

    public static class GroupCommitRequest {
        private final long nextOffset;
        // Indicate the GroupCommitRequest result: true or false
        private final CompletableFuture<PutMessageStatus> flushOKFuture = new CompletableFuture<>();
        private volatile int ackNums = 1;
        private final long deadLine;

        public GroupCommitRequest(long nextOffset, long timeoutMillis) {
            this.nextOffset = nextOffset;
            this.deadLine = System.nanoTime() + (timeoutMillis * 1_000_000);
        }

        public GroupCommitRequest(long nextOffset, long timeoutMillis, int ackNums) {
            this(nextOffset, timeoutMillis);
            this.ackNums = ackNums;
        }

        public long getNextOffset() {
            return nextOffset;
        }

        public int getAckNums() {
            return ackNums;
        }

        public long getDeadLine() {
            return deadLine;
        }

        public void wakeupCustomer(final PutMessageStatus status) {
            this.flushOKFuture.complete(status);
        }

        public CompletableFuture<PutMessageStatus> future() {
            return flushOKFuture;
        }
    }

    /**
     * GroupCommit Service
     */
    class GroupCommitService extends FlushCommitLogService {
        private volatile LinkedList<GroupCommitRequest> requestsWrite = new LinkedList<GroupCommitRequest>();
        private volatile LinkedList<GroupCommitRequest> requestsRead = new LinkedList<GroupCommitRequest>();
        private final PutMessageSpinLock lock = new PutMessageSpinLock();

        public synchronized void putRequest(final GroupCommitRequest request) {
            lock.lock();
            try {
                this.requestsWrite.add(request);
            } finally {
                lock.unlock();
            }
            this.wakeup();
        }

        private void swapRequests() {
            lock.lock();
            try {
                LinkedList<GroupCommitRequest> tmp = this.requestsWrite;
                this.requestsWrite = this.requestsRead;
                this.requestsRead = tmp;
            } finally {
                lock.unlock();
            }
        }

        private void doCommit() {
            if (!this.requestsRead.isEmpty()) {
                for (GroupCommitRequest req : this.requestsRead) {
                    // There may be a message in the next file, so a maximum of
                    // two times the flush
                    boolean flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                    for (int i = 0; i < 2 && !flushOK; i++) {
                        CommitLog.this.mappedFileQueue.flush(0);
                        flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                    }

                    req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
                }

                long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                if (storeTimestamp > 0) {
                    CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                }

                this.requestsRead = new LinkedList<>();
            } else {
                // Because of individual messages is set to not sync flush, it
                // will come to this process
                CommitLog.this.mappedFileQueue.flush(0);
            }
        }

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10);
                    this.doCommit();
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Under normal circumstances shutdown, wait for the arrival of the
            // request, and then flush
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            if (CommitLog.this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return CommitLog.this.defaultMessageStore.getBrokerConfig().getLoggerIdentifier() + GroupCommitService.class.getSimpleName();
            }
            return GroupCommitService.class.getSimpleName();
        }

        @Override
        public long getJoinTime() {
            return 1000 * 60 * 5;
        }
    }

    class GroupCheckService extends FlushCommitLogService {
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<GroupCommitRequest>();
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<GroupCommitRequest>();

        public boolean isAsyncRequestsFull() {
            return requestsWrite.size() > CommitLog.this.defaultMessageStore.getMessageStoreConfig().getMaxAsyncPutMessageRequests() * 2;
        }

        public synchronized boolean putRequest(final GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            if (hasNotified.compareAndSet(false, true)) {
                waitPoint.countDown(); // notify
            }
            boolean flag = this.requestsWrite.size() >
                CommitLog.this.defaultMessageStore.getMessageStoreConfig().getMaxAsyncPutMessageRequests();
            if (flag) {
                log.info("Async requests {} exceeded the threshold {}", requestsWrite.size(),
                    CommitLog.this.defaultMessageStore.getMessageStoreConfig().getMaxAsyncPutMessageRequests());
            }

            return flag;
        }

        private void swapRequests() {
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        private void doCommit() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    for (GroupCommitRequest req : this.requestsRead) {
                        // There may be a message in the next file, so a maximum of
                        // two times the flush
                        boolean flushOK = false;
                        for (int i = 0; i < 1000; i++) {
                            flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                            if (flushOK) {
                                break;
                            } else {
                                try {
                                    Thread.sleep(1);
                                } catch (Throwable ignored) {

                                }
                            }
                        }
                        req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    }

                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }

                    this.requestsRead.clear();
                }
            }
        }

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(1);
                    this.doCommit();
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Under normal circumstances shutdown, wait for the arrival of the
            // request, and then flush
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            if (CommitLog.this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return CommitLog.this.defaultMessageStore.getBrokerConfig().getLoggerIdentifier() + GroupCheckService.class.getSimpleName();
            }
            return GroupCheckService.class.getSimpleName();
        }

        @Override
        public long getJoinTime() {
            return 1000 * 60 * 5;
        }
    }

    class DefaultAppendMessageCallback implements AppendMessageCallback {
        // File at the end of the minimum fixed length empty
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        // Store the message content
        private final ByteBuffer msgStoreItemMemory;

        DefaultAppendMessageCallback() {
            this.msgStoreItemMemory = ByteBuffer.allocate(END_FILE_MIN_BLANK_LENGTH);
        }

        /**
         *
         *
         * @param fileFromOffset commitLog的文件名所表示的偏移量。
         * @param byteBuffer 缓冲区
         * @param maxBlank byteBuffer 剩余可以写入的最大空间
         * @param msgInner 消息
         * @param putMessageContext
         * @return
         */
        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
            final MessageExtBrokerInner msgInner, PutMessageContext putMessageContext) {
            // STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>

            // PHY OFFSET
            long wroteOffset = fileFromOffset + byteBuffer.position();

            // 创建全局唯一消息 ID。
            // 如果存储方为 IPV4，则消息 ID 有 16 字节：4IP+4端口号+8消息偏移量
            // 如果存储方为 IPV6，则消息 ID 有 28 字节，16IP+4端口+8消息偏移量
            Supplier<String> msgIdSupplier = () -> {
                int sysflag = msgInner.getSysFlag();
                int msgIdLen = (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 + 8 : 16 + 4 + 8;
                ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
                MessageExt.socketAddress2ByteBuffer(msgInner.getStoreHost(), msgIdBuffer);
                msgIdBuffer.clear();//because socketAddress2ByteBuffer flip the buffer
                msgIdBuffer.putLong(msgIdLen - 8, wroteOffset);
                return UtilAll.bytes2string(msgIdBuffer.array());
            };

            // Record ConsumeQueue information
            Long queueOffset = msgInner.getQueueOffset();

            // this msg maybe a inner-batch msg.
            short messageNum = getMessageNum(msgInner);

            // Transaction messages that require special handling
            final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
            switch (tranType) {
                // Prepared and Rollback message is not consumed, will not enter the consume queue
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    queueOffset = 0L;
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                default:
                    break;
            }

            ByteBuffer preEncodeBuffer = msgInner.getEncodedBuff();
            final int msgLen = preEncodeBuffer.getInt(0);

            // Determines whether there is sufficient free space
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                this.msgStoreItemMemory.clear();
                // 1 TOTALSIZE
                this.msgStoreItemMemory.putInt(maxBlank);
                // 2 MAGICCODE
                this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                // 3 The remaining space may be any value
                // Here the length of the specially set maxBlank
                final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset,
                    maxBlank, /* only wrote 8 bytes, but declare wrote maxBlank for compute write position */
                    msgIdSupplier, msgInner.getStoreTimestamp(),
                    queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            }

            int pos = 4 + 4 + 4 + 4 + 4;
            // 6 QUEUEOFFSET
            // 设置消息在 consumeQueue 的物理偏移量
            preEncodeBuffer.putLong(pos, queueOffset);
            pos += 8;
            // 7 PHYSICALOFFSET
            // 设置消息在 commitLog 的物理偏移量。
            preEncodeBuffer.putLong(pos, fileFromOffset + byteBuffer.position());
            int ipLen = (msgInner.getSysFlag() & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            // 8 SYSFLAG, 9 BORNTIMESTAMP, 10 BORNHOST, 11 STORETIMESTAMP
            pos += 8 + 4 + 8 + ipLen;
            // refresh store time stamp in lock
            preEncodeBuffer.putLong(pos, msgInner.getStoreTimestamp());

            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            CommitLog.this.getMessageStore().getPerfCounter().startTick("WRITE_MEMORY_TIME_MS");
            // Write messages to the queue buffer
            // 将消息写入 byteBuffer 即可。因为 byteBuffer 就是通过内存映射 commitLog 搞出来的。
            byteBuffer.put(preEncodeBuffer);
            CommitLog.this.getMessageStore().getPerfCounter().endTick("WRITE_MEMORY_TIME_MS");
            msgInner.setEncodedBuff(null);
            return new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgIdSupplier,
                msgInner.getStoreTimestamp(), queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills, messageNum);
        }

        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
            final MessageExtBatch messageExtBatch, PutMessageContext putMessageContext) {
            byteBuffer.mark();
            //physical offset
            long wroteOffset = fileFromOffset + byteBuffer.position();
            // Record ConsumeQueue information
            Long queueOffset = messageExtBatch.getQueueOffset();
            long beginQueueOffset = queueOffset;
            int totalMsgLen = 0;
            int msgNum = 0;

            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            ByteBuffer messagesByteBuff = messageExtBatch.getEncodedBuff();

            // 创建全局唯一消息 ID。
            // 如果存储方为 IPV4，则消息 ID 有 16 字节：4IP+4端口号+8消息偏移量
            // 如果存储方为 IPV6，则消息 ID 有 28 字节，16IP+4端口+8消息偏移量
            int sysFlag = messageExtBatch.getSysFlag();
            int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            Supplier<String> msgIdSupplier = () -> {
                int msgIdLen = storeHostLength + 8;
                int batchCount = putMessageContext.getBatchSize();
                long[] phyPosArray = putMessageContext.getPhyPos();
                ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
                MessageExt.socketAddress2ByteBuffer(messageExtBatch.getStoreHost(), msgIdBuffer);
                msgIdBuffer.clear();//because socketAddress2ByteBuffer flip the buffer

                StringBuilder buffer = new StringBuilder(batchCount * msgIdLen * 2 + batchCount - 1);
                for (int i = 0; i < phyPosArray.length; i++) {
                    msgIdBuffer.putLong(msgIdLen - 8, phyPosArray[i]);
                    String msgId = UtilAll.bytes2string(msgIdBuffer.array());
                    if (i != 0) {
                        buffer.append(',');
                    }
                    buffer.append(msgId);
                }
                return buffer.toString();
            };

            messagesByteBuff.mark();
            int index = 0;
            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                final int msgPos = messagesByteBuff.position();
                final int msgLen = messagesByteBuff.getInt();

                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if ((totalMsgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                    this.msgStoreItemMemory.clear();
                    // 1 TOTALSIZE
                    this.msgStoreItemMemory.putInt(maxBlank);
                    // 2 MAGICCODE
                    this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                    // 3 The remaining space may be any value
                    //ignore previous read
                    messagesByteBuff.reset();
                    // Here the length of the specially set maxBlank
                    byteBuffer.reset(); //ignore the previous appended messages
                    byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                    return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgIdSupplier, messageExtBatch.getStoreTimestamp(),
                        beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
                }
                //move to add queue offset and commitlog offset
                int pos = msgPos + 20;
                messagesByteBuff.putLong(pos, queueOffset);
                pos += 8;
                messagesByteBuff.putLong(pos, wroteOffset + totalMsgLen - msgLen);
                // 8 SYSFLAG, 9 BORNTIMESTAMP, 10 BORNHOST, 11 STORETIMESTAMP
                pos += 8 + 4 + 8 + bornHostLength;
                // refresh store time stamp in lock
                messagesByteBuff.putLong(pos, messageExtBatch.getStoreTimestamp());

                putMessageContext.getPhyPos()[index++] = wroteOffset + totalMsgLen - msgLen;
                queueOffset++;
                msgNum++;
                messagesByteBuff.position(msgPos + msgLen);
            }

            messagesByteBuff.position(0);
            messagesByteBuff.limit(totalMsgLen);
            byteBuffer.put(messagesByteBuff);
            messageExtBatch.setEncodedBuff(null);
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, totalMsgLen, msgIdSupplier,
                messageExtBatch.getStoreTimestamp(), beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            result.setMsgNum(msgNum);

            return result;
        }

    }

    public static class MessageExtEncoder {
        private ByteBuf byteBuf;
        // The maximum length of the message body.
        private int maxMessageBodySize;
        // The maximum length of the full message.
        private int maxMessageSize;
        MessageExtEncoder(final int maxMessageBodySize) {
            ByteBufAllocator alloc = UnpooledByteBufAllocator.DEFAULT;
            //Reserve 64kb for encoding buffer outside body
            int maxMessageSize = Integer.MAX_VALUE - maxMessageBodySize >= 64 * 1024 ?
                    maxMessageBodySize + 64 * 1024 : Integer.MAX_VALUE;
            byteBuf = alloc.directBuffer(maxMessageSize);
            this.maxMessageBodySize = maxMessageBodySize;
            this.maxMessageSize = maxMessageSize;
        }

        protected PutMessageResult encode(MessageExtBrokerInner msgInner) {
            this.byteBuf.clear();
            /**
             * Serialize message
             */
            final byte[] propertiesData =
                msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);

            final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

            if (propertiesLength > Short.MAX_VALUE) {
                log.warn("putMessage message properties length too long. length={}", propertiesData.length);
                return new PutMessageResult(PutMessageStatus.PROPERTIES_SIZE_EXCEEDED, null);
            }

            final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
            final int topicLength = topicData.length;

            final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;

            // 根据消息体、主题和属性的长度，结合消息存储格式，计算消息的总长度。
            final int msgLen = calMsgLength(msgInner.getSysFlag(), bodyLength, topicLength, propertiesLength);

            // Exceeds the maximum message body
            if (bodyLength > this.maxMessageBodySize) {
                CommitLog.log.warn("message body size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                    + ", maxMessageSize: " + this.maxMessageBodySize);
                return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
            }

            final long queueOffset = msgInner.getQueueOffset();

            // Exceeds the maximum message
            if (msgLen > this.maxMessageSize) {
                CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                    + ", maxMessageSize: " + this.maxMessageSize);
                return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
            }

            // 1 TOTALSIZE
            this.byteBuf.writeInt(msgLen);
            // 2 MAGICCODE
            this.byteBuf.writeInt(CommitLog.MESSAGE_MAGIC_CODE);
            // 3 BODYCRC
            this.byteBuf.writeInt(msgInner.getBodyCRC());
            // 4 QUEUEID
            this.byteBuf.writeInt(msgInner.getQueueId());
            // 5 FLAG
            this.byteBuf.writeInt(msgInner.getFlag());
            // 6 QUEUEOFFSET
            this.byteBuf.writeLong(queueOffset);
            // 7 PHYSICALOFFSET, need update later
            this.byteBuf.writeLong(0);
            // 8 SYSFLAG
            this.byteBuf.writeInt(msgInner.getSysFlag());
            // 9 BORNTIMESTAMP
            this.byteBuf.writeLong(msgInner.getBornTimestamp());

            // 10 BORNHOST
            ByteBuffer bornHostBytes = msgInner.getBornHostBytes();
            this.byteBuf.writeBytes(bornHostBytes.array());

            // 11 STORETIMESTAMP
            this.byteBuf.writeLong(msgInner.getStoreTimestamp());

            // 12 STOREHOSTADDRESS
            ByteBuffer storeHostBytes = msgInner.getStoreHostBytes();
            this.byteBuf.writeBytes(storeHostBytes.array());

            // 13 RECONSUMETIMES
            this.byteBuf.writeInt(msgInner.getReconsumeTimes());
            // 14 Prepared Transaction Offset
            this.byteBuf.writeLong(msgInner.getPreparedTransactionOffset());
            // 15 BODY
            this.byteBuf.writeInt(bodyLength);
            if (bodyLength > 0)
                this.byteBuf.writeBytes(msgInner.getBody());
            // 16 TOPIC
            this.byteBuf.writeByte((byte) topicLength);
            this.byteBuf.writeBytes(topicData);
            // 17 PROPERTIES
            this.byteBuf.writeShort((short) propertiesLength);
            if (propertiesLength > 0)
                this.byteBuf.writeBytes(propertiesData);

            return null;
        }

        protected ByteBuffer encode(final MessageExtBatch messageExtBatch, PutMessageContext putMessageContext) {
            this.byteBuf.clear();

            ByteBuffer messagesByteBuff = messageExtBatch.wrap();

            int totalLength = messagesByteBuff.limit();
            if (totalLength > this.maxMessageBodySize) {
                CommitLog.log.warn("message body size exceeded, msg body size: " + totalLength + ", maxMessageSize: " + this.maxMessageBodySize);
                throw new RuntimeException("message body size exceeded");
            }

            // properties from MessageExtBatch
            String batchPropStr = MessageDecoder.messageProperties2String(messageExtBatch.getProperties());
            final byte[] batchPropData = batchPropStr.getBytes(MessageDecoder.CHARSET_UTF8);
            int batchPropDataLen = batchPropData.length;
            if (batchPropDataLen > Short.MAX_VALUE) {
                CommitLog.log.warn("Properties size of messageExtBatch exceeded, properties size: {}, maxSize: {}.", batchPropDataLen, Short.MAX_VALUE);
                throw new RuntimeException("Properties size of messageExtBatch exceeded!");
            }
            final short batchPropLen = (short) batchPropDataLen;

            int batchSize = 0;
            while (messagesByteBuff.hasRemaining()) {
                batchSize++;
                // 1 TOTALSIZE
                messagesByteBuff.getInt();
                // 2 MAGICCODE
                messagesByteBuff.getInt();
                // 3 BODYCRC
                messagesByteBuff.getInt();
                // 4 FLAG
                int flag = messagesByteBuff.getInt();
                // 5 BODY
                int bodyLen = messagesByteBuff.getInt();
                int bodyPos = messagesByteBuff.position();
                int bodyCrc = UtilAll.crc32(messagesByteBuff.array(), bodyPos, bodyLen);
                messagesByteBuff.position(bodyPos + bodyLen);
                // 6 properties
                short propertiesLen = messagesByteBuff.getShort();
                int propertiesPos = messagesByteBuff.position();
                messagesByteBuff.position(propertiesPos + propertiesLen);
                boolean needAppendLastPropertySeparator = propertiesLen > 0 && batchPropLen > 0
                    && messagesByteBuff.get(messagesByteBuff.position() - 1) != MessageDecoder.PROPERTY_SEPARATOR;

                final byte[] topicData = messageExtBatch.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);

                final int topicLength = topicData.length;

                int totalPropLen = needAppendLastPropertySeparator ? propertiesLen + batchPropLen + 1
                    : propertiesLen + batchPropLen;
                final int msgLen = calMsgLength(messageExtBatch.getSysFlag(), bodyLen, topicLength, totalPropLen);

                // 1 TOTALSIZE
                this.byteBuf.writeInt(msgLen);
                // 2 MAGICCODE
                this.byteBuf.writeInt(CommitLog.MESSAGE_MAGIC_CODE);
                // 3 BODYCRC
                this.byteBuf.writeInt(bodyCrc);
                // 4 QUEUEID
                this.byteBuf.writeInt(messageExtBatch.getQueueId());
                // 5 FLAG
                this.byteBuf.writeInt(flag);
                // 6 QUEUEOFFSET
                this.byteBuf.writeLong(0);
                // 7 PHYSICALOFFSET
                this.byteBuf.writeLong(0);
                // 8 SYSFLAG
                this.byteBuf.writeInt(messageExtBatch.getSysFlag());
                // 9 BORNTIMESTAMP
                this.byteBuf.writeLong(messageExtBatch.getBornTimestamp());

                // 10 BORNHOST
                ByteBuffer bornHostBytes = messageExtBatch.getBornHostBytes();
                this.byteBuf.writeBytes(bornHostBytes.array());

                // 11 STORETIMESTAMP
                this.byteBuf.writeLong(messageExtBatch.getStoreTimestamp());

                // 12 STOREHOSTADDRESS
                ByteBuffer storeHostBytes = messageExtBatch.getStoreHostBytes();
                this.byteBuf.writeBytes(storeHostBytes.array());

                // 13 RECONSUMETIMES
                this.byteBuf.writeInt(messageExtBatch.getReconsumeTimes());
                // 14 Prepared Transaction Offset, batch does not support transaction
                this.byteBuf.writeLong(0);
                // 15 BODY
                this.byteBuf.writeInt(bodyLen);
                if (bodyLen > 0)
                    this.byteBuf.writeBytes(messagesByteBuff.array(), bodyPos, bodyLen);
                // 16 TOPIC
                this.byteBuf.writeByte((byte) topicLength);
                this.byteBuf.writeBytes(topicData);
                // 17 PROPERTIES
                this.byteBuf.writeShort((short) totalPropLen);
                if (propertiesLen > 0) {
                    this.byteBuf.writeBytes(messagesByteBuff.array(), propertiesPos, propertiesLen);
                }
                if (batchPropLen > 0) {
                    if (needAppendLastPropertySeparator) {
                        this.byteBuf.writeByte((byte) MessageDecoder.PROPERTY_SEPARATOR);
                    }
                    this.byteBuf.writeBytes(batchPropData, 0, batchPropLen);
                }
            }
            putMessageContext.setBatchSize(batchSize);
            putMessageContext.setPhyPos(new long[batchSize]);

            return this.byteBuf.nioBuffer();
        }

        public ByteBuffer getEncoderBuffer() {
            return this.byteBuf.nioBuffer();
        }

        public int getMaxMessageBodySize() {
            return this.maxMessageBodySize;
        }

        public void updateEncoderBufferCapacity(int newMaxMessageBodySize) {
            this.maxMessageBodySize = newMaxMessageBodySize;
            //Reserve 64kb for encoding buffer outside body
            this.maxMessageSize = Integer.MAX_VALUE - newMaxMessageBodySize >= 64 * 1024 ?
                    this.maxMessageBodySize + 64 * 1024 : Integer.MAX_VALUE;
            this.byteBuf.capacity(this.maxMessageSize);
        }
    }

    interface FlushManager {
        void start();

        void shutdown();

        void wakeUpFlush();

        void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt);

        CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, MessageExt messageExt);
    }

    class DefaultFlushManager implements FlushManager {

        private final FlushCommitLogService flushCommitLogService;

        //If TransientStorePool enabled, we must flush message to FileChannel at fixed periods
        private final FlushCommitLogService commitLogService;

        public DefaultFlushManager() {
            if (FlushDiskType.SYNC_FLUSH == CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
                this.flushCommitLogService = new CommitLog.GroupCommitService();
            } else {
                this.flushCommitLogService = new CommitLog.FlushRealTimeService();
            }

            this.commitLogService = new CommitLog.CommitRealTimeService();
        }

        @Override
        public void start() {
            this.flushCommitLogService.start();

            if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                this.commitLogService.start();
            }
        }

        public void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult,
            MessageExt messageExt) {
            // Synchronization flush
            if (FlushDiskType.SYNC_FLUSH == CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
                final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
                if (messageExt.isWaitStoreMsgOK()) {
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(), CommitLog.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                    service.putRequest(request);
                    CompletableFuture<PutMessageStatus> flushOkFuture = request.future();
                    PutMessageStatus flushStatus = null;
                    try {
                        flushStatus = flushOkFuture.get(CommitLog.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout(),
                            TimeUnit.MILLISECONDS);
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        //flushOK=false;
                    }
                    if (flushStatus != PutMessageStatus.PUT_OK) {
                        log.error("do groupcommit, wait for flush failed, topic: " + messageExt.getTopic() + " tags: " + messageExt.getTags()
                            + " client address: " + messageExt.getBornHostString());
                        putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    }
                } else {
                    service.wakeup();
                }
            }
            // Asynchronous flush
            else {
                if (!CommitLog.this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                    flushCommitLogService.wakeup();
                } else {
                    commitLogService.wakeup();
                }
            }
        }

        @Override
        public CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, MessageExt messageExt) {
            // Synchronization flush
            if (FlushDiskType.SYNC_FLUSH == CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
                final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
                if (messageExt.isWaitStoreMsgOK()) {
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(), CommitLog.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                    flushDiskWatcher.add(request);
                    service.putRequest(request);
                    return request.future();
                } else {
                    service.wakeup();
                    return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
                }
            }
            // Asynchronous flush
            else {
                if (!CommitLog.this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                    flushCommitLogService.wakeup();
                } else {
                    commitLogService.wakeup();
                }
                return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
            }
        }

        @Override
        public void wakeUpFlush() {
            // now wake up flush thread.
            flushCommitLogService.wakeup();
        }

        @Override
        public void shutdown() {
            if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                this.commitLogService.shutdown();
            }

            this.flushCommitLogService.shutdown();
        }

    }

    public int getCommitLogSize() {
        return commitLogSize;
    }

    public MappedFileQueue getMappedFileQueue() {
        return mappedFileQueue;
    }

    public MessageStore getMessageStore() {
        return defaultMessageStore;
    }

    @Override
    public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {
        this.getMappedFileQueue().swapMap(reserveNum, forceSwapIntervalMs, normalSwapIntervalMs);
    }

    public boolean isMappedFilesEmpty() {
        return this.mappedFileQueue.isMappedFilesEmpty();
    }

    @Override
    public void cleanSwappedMap(long forceCleanSwapIntervalMs) {
        this.getMappedFileQueue().cleanSwappedMap(forceCleanSwapIntervalMs);
    }

    static class PutMessageThreadLocal {
        private MessageExtEncoder encoder;
        private StringBuilder keyBuilder;

        PutMessageThreadLocal(int maxMessageBodySize) {
            encoder = new MessageExtEncoder(maxMessageBodySize);
            keyBuilder = new StringBuilder();
        }

        public MessageExtEncoder getEncoder() {
            return encoder;
        }

        public StringBuilder getKeyBuilder() {
            return keyBuilder;
        }
    }
}
