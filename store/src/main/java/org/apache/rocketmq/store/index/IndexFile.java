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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.List;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;

/**
 * ConsumeQueue 是 RocketMQ 专门为消息订阅构建的索引文件，目的是提高根据主题与消息队列检索消息的速度。
 * 另外，RocketMQ 引入哈希所以机制为消息建立索引。Index 文件使用 HashMap 的原理：哈希槽 + 冲突链表。
 *
 * |----文件头----|----500万个哈希槽----|----2000万个Index条目----|
 *
 *
 * 1、文件头（40 Byte）：
 *      | beginTimestamp | endTimestamp | beginPhyOffset | endPhyOffset | hashSlotCount | indexCount |
 *      |     8 Byte     |    8 Byte    |     8 Byte     |    8 Byte    |     4 Byte    |   4 Byte   |
 * 2、哈希槽（4 Byte * 5_000_000）：
 *      每个哈希槽都是 4 Byte，指向 Index 条目。
 * 3、Index 条目（20 Byte * 20_000_000）:
 *      | hashCode | phyOffset | timeDif | pre index no |
 *      |  4 Byte  |  8 Byte   | 4 Byte  |    4 Byte    |
 *
 * 文件头字段详解：
 *      beginTimestamp：Index 文件中消息的最小存储时间
 *      endTimestamp：Index 文件中消息的最大存储时间
 *      beginPhyOffset：Index 文件中消息的最小物理偏移量（CommitLog 文件的偏移量）
 *      endPhyOffset：Index 文件中消息的最大物理偏移量（CommitLog 文件的偏移量）
 *      hashSlotCount：IhashSlot 个数，并不是哈希槽使用的个数，这个字段没什么意义，本来就定死了 500 万个槽
 *      indexCount：Index 条目列表当前已使用的个数，Index 条目在 Index 条目列表中按顺序存储
 *
 * 哈希槽存储的是落在该哈希槽的最新的 Index 索引（即头插法）
 *
 * Index 条目字段详解：
 *      hashCode：key 的哈希吗
 *      phyOffset：消息对应的物理偏移量
 *      timeDif：该消息存储时间与第一条消息的时间戳的差值，若小于 0，则该消息无效。单位：秒
 *      pre index no：该条目的前一条记录的 Index 索引，当出现哈希冲突时，构建链表结构
 *
 * IndexFile 将消息索引 key 和消息物理偏移量存入 Index 文件的方法为：{@link IndexFile#putKey(String, long, long)}
 * IndexFile 根据索引 key 查找消息的实现方法为：{@link IndexFile#selectPhyOffset(List, String, int, long, long)}
 *
 * 注意：IndexFile 中并没有存储 key，仅仅是根据 hashCode 进行聚簇，所以真正过滤某个 key 的时候还需要进一步 equals 过滤。
 *
 */
public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int hashSlotSize = 4;
    private static int indexSize = 20;
    private static int invalidIndex = 0;
    private final int hashSlotNum;
    private final int indexNum;
    private final int fileTotalSize;
    private final MappedFile mappedFile;
    private final MappedByteBuffer mappedByteBuffer;
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        this.fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new DefaultMappedFile(fileName, fileTotalSize);
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public int getFileSize() {
        return this.fileTotalSize;
    }

    public void load() {
        this.indexHeader.load();
    }

    public void shutdown() {
        this.flush();
        UtilAll.cleanBuffer(this.mappedByteBuffer);
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * 1、当前已使用条目大于、等于允许最大条目时，返回 false，表示当前文件已经写满。如果没有写满则根据 key 算出 hashCode，并找到对应的哈希槽。
     * 2、读取哈希槽的存储的数据，如果存的数据小于 0 或者大于当前 Index 文件中的索，则将 slotValue 设置为 0。
     * 3、计算待存储消息的时间戳与第一条消息时间戳的差值，并转换成秒。
     * 4、将条目存储到 Index 文件中。
     *    a.计算新添加条目的起始物理偏移量：头部字节长度 + 哈希槽数量 * 单个哈希槽大小 + 当前 Index 条目个数 * 单个 Index 条目大小
     *    b.一次将哈希码、消息物理偏移量、消息存储时间戳与 Index 文件时间戳差值、当前哈希槽的值存入 MappedByteBuffer。
     *    c.将当前 Index 文件中包含的条目数量存入哈希槽中，覆盖原先哈希槽的值。
     * 5、更新文件索引头信息。如果当前文件只包含一个条目，则更新 beginPhyOffset、beginTimestamp、endPhyOffset、endTimestamp 以及当前
     *  文件使用索引条目等信息。
     *
     * @param key 消息索引
     * @param phyOffset 消息物理偏移量
     * @param storeTimestamp 消息存储时间
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            try {

                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;

                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                if (invalidIndex == slotValue) {
                    this.indexHeader.incHashSlotCount();
                }
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0) {
            keyHashPositive = 0;
        }
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    /**
     * 1、根据 key 算出哈希吗，然后定位到对应的哈希槽
     * 2、判断哈希槽的数据，如果小于 1 或者大于当前索引条目个数，表示该哈希槽没有数据。
     * 3、因为会存在哈希冲突，所以根据 slotValue 定位该哈希槽最新的一个 Index 条目，
     *  将存储的物理偏移量加入 phyOffsets，然后继续验证 Index 条目中存储的上一个
     *  Index 条目，如果大于等于1并且小于当前文件最大条目数，则继续查找，否则结束查找。
     * 4、根据 Index 下标定位到条目的起始物理偏移量，然后以此读取哈希码、物理偏移量、
     *  时间戳、上一个条目的 Index 下标。
     * 5、如果存储的时间戳小于 0，则直接查找结束。如果哈希匹配并且消息存储时间介于查找
     *  时间 [begin, end] 之间，则将消息物理偏移量加入 phyOffsets，并验证条目的前
     *  一个 Index 索引，如果索引大于等于 1 并且小于 Index 条目数，则继续查找。
     *
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
                                final long begin, final long end) {
        if (this.mappedFile.hold()) {
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            try {
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                this.mappedFile.release();
            }
        }
    }
}
