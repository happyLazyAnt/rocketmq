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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

public class TransientStorePool {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final int poolSize;
    private final int fileSize;
    private final Deque<ByteBuffer> availableBuffers;
    private final DefaultMessageStore messageStore;
    private volatile boolean isRealCommit = true;

    public TransientStorePool(final DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
        this.poolSize = messageStore.getMessageStoreConfig().getTransientStorePoolSize();
        this.fileSize = messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * It's a heavy init method.
     */
    public void init() {
        for (int i = 0; i < poolSize; i++) {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            /**
             * TODO: ZXZ
             *
             * LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize)) 这段代码是使用Java的JNI（Java Native Interface）调用C库函数的一个示例，具体是调用了Linux系统中的mlock函数。这个调用的作用是锁定一段内存区域，防止其被交换到磁盘上，从而确保这段内存数据始终驻留在物理内存中，提升访问速度和安全性。下面是关于这个调用的几个关键点：
             * LibC.INSTANCE: 这部分表示通过JNI调用C库（通常是glibc或其他兼容库），INSTANCE是JNA（Java Native Access）库中用来访问本地库函数的一个静态成员。JNA允许Java程序无需编写JNI代码就能直接调用Native库中的函数。
             * mlock: 是Linux系统提供的一个系统调用，用于锁定指定地址范围内的内存页，确保这些页不会被操作系统交换到交换空间（即磁盘上的SWAP分区或文件）。这对于需要高性能访问或敏感数据保护的场景非常有用。
             * pointer: 是一个指向要锁定内存区域起始地址的指针。在Java中，这通常通过ByteBuffer的address()方法获取，当你使用了如DirectByteBuffer这样的直接缓冲区时。直接缓冲区可以直接映射到内存，提供给底层系统调用使用。
             * new NativeLong(fileSize): 表示要锁定的内存区域的大小，单位通常是字节。NativeLong是JNA中用于与本地代码交互的类型，确保整数大小与目标平台匹配。
             * 注意事项:
             * 使用mlock需要相应的权限，通常只有root用户或具有CAP_IPC_LOCK能力的进程才能调用成功。
             * 锁定大量内存可能导致系统内存资源紧张，影响其他进程或服务。
             * 应该在不再需要锁定的内存时，使用munlock函数解锁内存，避免不必要的资源占用。
             * 由于直接操作内存和系统调用涉及底层细节，错误使用可能引发稳定性问题或安全风险，需谨慎处理。
             *
             * 注意：使用内存池，需要确保有足够的内存空间
             */
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

            availableBuffers.offer(byteBuffer);
        }
    }

    public void destroy() {
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    public void returnBuffer(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);
        this.availableBuffers.offerFirst(byteBuffer);
    }

    public ByteBuffer borrowBuffer() {
        ByteBuffer buffer = availableBuffers.pollFirst();
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    public int availableBufferNums() {
        if (messageStore.isTransientStorePoolEnable()) {
            return availableBuffers.size();
        }
        return Integer.MAX_VALUE;
    }

    public boolean isRealCommit() {
        return isRealCommit;
    }

    public void setRealCommit(boolean realCommit) {
        isRealCommit = realCommit;
    }
}
