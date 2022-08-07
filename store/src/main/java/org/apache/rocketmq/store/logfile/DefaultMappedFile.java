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
package org.apache.rocketmq.store.logfile;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.AppendMessageCallback;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageContext;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.TransientStorePool;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

public class DefaultMappedFile extends AbstractMappedFile {
    /**
     * 操作系统每页的大小，默认4K
     */
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 当前JVM实例中 MappedFile 的虚拟内存
     */
    protected static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    /**
     * 当前JVM实例中 MappedFile 对象个数。
     */
    protected static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
    /**
     * 当前文件的写指针，从0开始
     */
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    /**
     * 当前文件的提交指针，如果开启 transientStorePollEnable，
     * 则数据会存储在 TransientStorePool 中，然后提交到内存映射 ByteBuffer 中，再写入磁盘。
     */
    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    /**
     * 该指针之前的数据已持久化存储到磁盘中
     */
    protected final AtomicInteger flushedPosition = new AtomicInteger(0);
    /**
     * 文件大小
     */
    protected int fileSize;
    /**
     * 文件通道
     */
    protected FileChannel fileChannel;
    /**
     * 堆外内存 ByteBuffer，如果不为 null，数据首先将存储到该 Buffer 中，
     * 然后由 Commit 线程将数据提交到 MappedFile 创建的 FileChannel 中，
     * 在通过 Flush 线程间数据持久化到磁盘中。
     * transientStorePoolEnable 为 true 时不为 null。
     */
    protected ByteBuffer writeBuffer = null;
    /**
     * 堆外内存池，该内存池中的内存会提供内存锁机制。transientStorePoolEnable 为 true 时启用。
     */
    protected TransientStorePool transientStorePool = null;
    /**
     * 文件名称
     */
    protected String fileName;
    /**
     * 文件的初始偏移量。
     */
    protected long fileFromOffset;
    /**
     * 物理文件
     */
    protected File file;
    /**
     * 物理文件对应的内存映射 Buffer。
     * this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
     * 这玩意就将 fileChannel 整个文件映射出来了，那么直接向 fileChannel write 数据这里也能实时看到。
     */
    protected MappedByteBuffer mappedByteBuffer;
    /**
     * 文件最后一次写入内容的时间
     */
    protected volatile long storeTimestamp = 0;
    /**
     * 是否是 MappedFileQueue 中的第一个文件呢。
     */
    protected boolean firstCreateInQueue = false;
    private long lastFlushTime = -1L;

    protected MappedByteBuffer mappedByteBufferWaitToClean = null;
    protected long swapMapTime = 0L;
    protected long mappedByteBufferAccessCountSinceLastSwap = 0L;

    public DefaultMappedFile() {
    }

    public DefaultMappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public DefaultMappedFile(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    @Override
    public void init(final String fileName, final int fileSize,
                     final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        // 如果 transientStorePoolEnable 为 true，则初始化堆外内存 Buffer，该 Buffer 从 transientStorePool 获取。
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        UtilAll.ensureDirOK(this.file.getParent());

        try {
            // 使用 RandomAccessFile 创建文件通道，并将文件内容使用 NIO 的内存映射 Buffer 将文件映射到内存中。
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    @Override
    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public boolean getData(int pos, int size, ByteBuffer byteBuffer) {
        if (byteBuffer.remaining() < size) {
            return false;
        }

        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {

            if (this.hold()) {
                try {
                    int readNum = fileChannel.read(byteBuffer, pos);
                    return size == readNum;
                } catch (Throwable t) {
                    log.warn("Get data failed pos:{} size:{} fileFromOffset:{}", pos, size, this.fileFromOffset);
                    return false;
                } finally {
                    this.release();
                }
            } else {
                log.debug("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return false;
    }

    @Override
    public int getFileSize() {
        return fileSize;
    }

    @Override
    public FileChannel getFileChannel() {
        return fileChannel;
    }

    @Override
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb,
            PutMessageContext putMessageContext) {
        return appendMessagesInner(msg, cb, putMessageContext);
    }

    @Override
    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb,
            PutMessageContext putMessageContext) {
        return appendMessagesInner(messageExtBatch, cb, putMessageContext);
    }

    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb,
            PutMessageContext putMessageContext) {
        assert messageExt != null;
        assert cb != null;

        // 1、首先获取 MappedFile 当前的写指针
        int currentPos = this.wrotePosition.get();

        // 2、如果 currentPos 大于或等于文件大小就表明文件写满了，返回 AppendMessageStatus.UNKNOWN_ERROR 结果。
        // 否则通过 slice() 方法创建一个与原 ByteBuffer 共享的内存区，且拥有独立的 position、limit 等指针，并设置
        // position 为当前写指针。
        if (currentPos < this.fileSize) {
            ByteBuffer byteBuffer = appendMessageBuffer().slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            if (messageExt instanceof MessageExtBatch && !((MessageExtBatch) messageExt).isInnerBatch()) {
                // traditional batch message
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                        (MessageExtBatch) messageExt, putMessageContext);
            } else if (messageExt instanceof MessageExtBrokerInner) {
                // traditional single message or newly introduced inner-batch message
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                        (MessageExtBrokerInner) messageExt, putMessageContext);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            this.wrotePosition.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    protected ByteBuffer appendMessageBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return writeBuffer != null ? writeBuffer : this.mappedByteBuffer;
    }

    @Override
    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    @Override
    public boolean appendMessage(final byte[] data) {
        return appendMessage(data, 0, data.length);
    }

    @Override
    public boolean appendMessage(ByteBuffer data) {
        int currentPos = this.wrotePosition.get();
        int remaining = data.remaining();

        if ((currentPos + remaining) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                while (data.hasRemaining()) {
                    this.fileChannel.write(data);
                }
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(remaining);
            return true;
        }
        return false;
    }

    /**
     * Content of data from offset to offset + length will be written to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    @Override
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                ByteBuffer buf = this.mappedByteBuffer.slice();
                buf.position(currentPos);
                buf.put(data, offset, length);
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * 将内存中的数据持久化道磁盘中。
     * 如果 writeBuffer 不为空，则 flushedPosition 应等于上一次 commit 指针。因为上一次提交的数据就是进入 MappedByteBuffer 中的数据。
     * 如果 writeBuffer 为空，表示属实直接是进入 MappedByteBuffer 的，wrotePosition 代表的是 MappedByteBuffer 中的指针，故设置
     * flushedPosition 为 wrotePosition。
     *
     * @return The current flushed position
     */
    @Override
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    this.mappedByteBufferAccessCountSinceLastSwap++;

                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                    this.lastFlushTime = System.currentTimeMillis();
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * 执行提交操作，commitLeastPages 为本次提交的最小页数，如果待
     * 提交数据不满足 commitLeastPages 则不执行本次提交操作，等待下
     * 次提交。writeBuffer 如果为空，直接返回 wrotePosition 指针而
     * 无需执行 commit 操作。因为此时 transientStorePoolEnable
     * 为 true，应由后台线程 commit。
     *
     * @param commitLeastPages the least pages to commit
     * @return
     */
    @Override
    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0();
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    /**
     * 首先创建 writeBuffer 的共享缓冲区，然后将新创建缓冲区的 position 设置为上一次提交的位置(committedPosition)，
     * 再将 limit 设置为 wrotePosition(当前最大有效数据指针)，接着把 committedPosition 到 wrotePosition 的数据
     * 写入到 FileChannel 中，最后更新 committedPosition 为 wrotePosition。
     * commit 的作用是将 MappedFile#writeBuffer 中的数据提交到文件通道 FileChannel 中。
     *
     * ByteBuffer#slice()方法创建一个共享缓冲区，与原 ByteBuffer 共享内存但是独自维护一套指针(position，mark，limit)
     */
    protected void commit0() {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - lastCommittedPosition > 0) {
            try {
                ByteBuffer byteBuffer = writeBuffer.slice();
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get();
        int write = getReadPosition();

        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    /**
     * 判断是否执行 commit 操作。如果文件已满，返回 true。如果 commitLeastPages 大于 0，
     * 则计算 wrotePosition（当前writeBuffer的写指针）与上一次提交的指针(committedPosition)
     * 的差值，将其除以 OS_PAGE_SIZE 得到当前脏页的数量，如果大于 commitLeastPages 则返回 true。
     * 如果 commitLeastPages 小于 0，则表示只要存在脏页就提交。
     */
    protected boolean isAbleToCommit(final int commitLeastPages) {
        int commit = this.committedPosition.get();
        int write = this.wrotePosition.get();

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (commit / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > commit;
    }

    @Override
    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    @Override
    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    @Override
    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    @Override
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                this.mappedByteBufferAccessCountSinceLastSwap++;

                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    /**
     * 首先查找 pos 到当前最大指针之间的数据，因为在整个写入期间都未曾改变 MappedByteBuffer 的指针，
     * 所以 mappedByteBuffer.slice() 方法返回的共享缓冲区空间为这个 MappedFile。然后通过设置slice
     * 出来的 byteBuffer 的 position 为待查找的值，读取字节为当前可读字节长度，最终返回的 ByteBuffer
     * 的 limit(可读的最大长度) 为 size。整个共享缓冲区的内容量为 MappedFile#fileSize，故在操作
     * SelectMappedBufferResult 时不能对包含在里面的 ByteBuffer 调用 flip() 方法。
     *
     * 所以：操作 ByteBuffer 时如果使用了 slice() 方法，对其 ByteBuffer 进行读取时一般手动指定
     * position 和 limit 指针，而不是调用 flip() 方法切换读写状态。
     *
     * @param pos the given position
     * @return
     */
    @Override
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                this.mappedByteBufferAccessCountSinceLastSwap++;
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    /**
     * 1、如果 available 为 true，表示 MappedFile 当前可用，无需清理，返回 false。
     * 2、如果资源已被清理，返回 true。
     * 3、清理资源并维护统计数值，返回 true。
     *
     * @param currentRef
     * @return
     */
    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }

        UtilAll.cleanBuffer(this.mappedByteBuffer);
        UtilAll.cleanBuffer(this.mappedByteBufferWaitToClean);
        this.mappedByteBufferWaitToClean = null;
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    /**
     * 入参intervalForcibly：表示拒绝被销毁的最大存活时间。
     * 1、关闭 MappedFile。除此调用时 this.
     * 2、判断是否清理完成，判断标准是引用次数小于、等于0并且 cleanupOver 为 true。cleanupOver == true 的触发条件是第一步的shutdown方法成功的将 MappedByteBuffer 释放掉。
     * 3、关闭文件通道，删除物理文件。
     *
     *
     * @param intervalForcibly If {@code true} then this method will destroy the file forcibly and ignore the reference
     * @return
     */
    @Override
    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                long lastModified = getLastModifiedTimestamp();
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                        + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                        + this.getFlushedPosition() + ", "
                        + UtilAll.computeElapsedTimeMilliseconds(beginTime)
                        + "," + (System.currentTimeMillis() - lastModified));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                    + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    @Override
    public int getWrotePosition() {
        return wrotePosition.get();
    }

    @Override
    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * 获取最大有效数据偏移量，也就是当前文件最大的可读指针。
     * 如过 writeBuffer != null 则表明当前启用了 transientStorePool 机制，此时获取commit线程最后一次的提交位置 committedPosition。
     * 否则就直接返回 wrotePosition 即可。
     * 在 MappedFile 的设计中，只有提交了的数据(写入 MappedByteBuffer 或 FileChannel 中的数据)才是安全有效的。
     *
     * @return The max position which have valid data
     */
    @Override
    public int getReadPosition() {
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    @Override
    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    @Override
    public void warmMappedFile(FlushDiskType type, int pages) {
        this.mappedByteBufferAccessCountSinceLastSwap++;

        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += DefaultMappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    @Override
    public boolean swapMap() {
        if (getRefCount() == 1 && this.mappedByteBufferWaitToClean == null) {

            if (!hold()) {
                log.warn("in swapMap, hold failed, fileName: " + this.fileName);
                return false;
            }
            try {
                this.mappedByteBufferWaitToClean = this.mappedByteBuffer;
                this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
                this.mappedByteBufferAccessCountSinceLastSwap = 0L;
                this.swapMapTime = System.currentTimeMillis();
                log.info("swap file " + this.fileName + " success.");
                return true;
            } catch (Exception e) {
                log.error("swapMap file " + this.fileName + " Failed. ", e);
            } finally {
                this.release();
            }
        } else {
            log.info("Will not swap file: " + this.fileName + ", ref=" + getRefCount());
        }
        return false;
    }

    @Override
    public void cleanSwapedMap(boolean force) {
        try {
            if (this.mappedByteBufferWaitToClean == null) {
                return;
            }
            long minGapTime = 120 * 1000L;
            long gapTime = System.currentTimeMillis() - this.swapMapTime;
            if (!force && gapTime < minGapTime) {
                Thread.sleep(minGapTime - gapTime);
            }
            UtilAll.cleanBuffer(this.mappedByteBufferWaitToClean);
            mappedByteBufferWaitToClean = null;
            log.info("cleanSwapedMap file " + this.fileName + " success.");
        } catch (Exception e) {
            log.error("cleanSwapedMap file " + this.fileName + " Failed. ", e);
        }
    }

    @Override
    public long getRecentSwapMapTime() {
        return 0;
    }

    @Override
    public long getMappedByteBufferAccessCountSinceLastSwap() {
        return this.mappedByteBufferAccessCountSinceLastSwap;
    }

    @Override
    public long getLastFlushTime() {
        return this.lastFlushTime;
    }

    @Override
    public String getFileName() {
        return fileName;
    }

    @Override
    public MappedByteBuffer getMappedByteBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return mappedByteBuffer;
    }

    @Override
    public ByteBuffer sliceByteBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return this.mappedByteBuffer.slice();
    }

    @Override
    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    @Override
    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    @Override
    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    @Override
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    @Override
    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    @Override
    public File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }

}
