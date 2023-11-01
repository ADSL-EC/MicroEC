/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.crail.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.crail.*;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.core.*;
import org.apache.crail.memory.OffHeapBuffer;
import org.apache.crail.utils.CodingCoreCache;
import org.apache.crail.utils.CrailUtils;
import org.apache.crail.utils.RingBuffer;


public class CrailBenchmark {
    private int warmup;
    private CrailConfiguration conf;
    private CrailStore fs;
    private long[] everyLoopLatency;
    private int warmupTimes;
    private double avgLoopLatency;
    private double iops;
    private int minBlockSize;
    private int maxSmallObjSize;
    private long blockingBound;

    private List<Long> encodingLatencies;
    private List<Long> mergeLatencies;
    private List<Long> transferLatencies;
    private List<Long> ackLatencies;

    // degrade and recovery test
    private int degradeNum;
    private int[] degradeIndices;

    public CrailBenchmark(int warmup) throws Exception {
        this.warmup = warmup;
        this.conf = CrailConfiguration.createConfigurationFromFile();
        this.fs = null;
        this.warmupTimes = 0;

        // minimum size of a coding block
        minBlockSize = 4096;
        // max size of small obj size, bind a fix core for small obj
        maxSmallObjSize = 128 * 1024;
        // maximum blocking time of retry reading hasCoded
        // 1s
        blockingBound = 1000000000;

        // us
        avgLoopLatency = 0.0;
        iops = 0.0;

        // breakdown measurements
        encodingLatencies = new ArrayList<>();
        mergeLatencies = new ArrayList<>();
        transferLatencies = new ArrayList<>();
        ackLatencies = new ArrayList<>();

        // degrade and recovery test
        // 1 lost
        degradeNum = 1;
        degradeIndices = new int[degradeNum];
        degradeIndices[0] = 0;

        // 2 lost
//        degradeNum = 2;
//        degradeIndices = new int[degradeNum];
//        degradeIndices[0] = 0;
//        degradeIndices[1] = 1;

        // TODO CrailConstants here is default value defined in CrailConstant.java
        // TODO and not being updated crail-site.conf
        // TODO Values in CrailConstants will be update after open()
        if (degradeNum > CrailConstants.REPLICATION_FACTOR || degradeNum > CrailConstants.ERASURECODING_M) {
            throw new Exception("degradeNum>CrailConstants.REPLICATION_FACTOR or degradeNum > CrailConstants.ERASURECODING_M");
        }
    }

    private void open() throws Exception {
        if (fs == null) {
            this.fs = CrailStore.newInstance(conf);
        }
    }

    private void close() throws Exception {
        if (fs != null) {
            fs.close();
            fs = null;
        }
    }

    public double getBreakdownOperationsLatency(List<Long> latencies, String mark) {
        double sum = 0;

        for (int i = 0; i < latencies.size(); i++) {
            System.out.println("Loop " + i + " " + mark + " " + latencies.get(i) / 1000.0);

            if (i >= warmupTimes) {
                sum += latencies.get(i) / 1000.0;
            }
        }

        return sum / (latencies.size() - warmupTimes);
    }

    public void getBreakdownLatency() {
        System.out.println("Encoding avg latency (us) " + getBreakdownOperationsLatency(encodingLatencies, "encoding (us)"));
        System.out.println("Merge avg latency (us) " + getBreakdownOperationsLatency(mergeLatencies, "merge (us)"));
        System.out.println("Transfer avg latency (us) " + getBreakdownOperationsLatency(transferLatencies, "transfer (us)"));
    }

    public void getAvgLoopLatency() {
        for (int i = 0; i < everyLoopLatency.length; i++) {
            System.out.println("Loop " + i + " (us) " + everyLoopLatency[i] / 1000.0);
        }

        System.out.println("Warmup times " + this.warmupTimes);

        if (everyLoopLatency.length <= warmupTimes) {
            System.out.println("everyLoopLatency.length " + everyLoopLatency.length + " <= warmupTimes " + warmupTimes);
            return;
        }

        // ns
        double sum = 0.0;
        for (int i = warmupTimes; i < everyLoopLatency.length; i++) {
            sum += everyLoopLatency[i];
        }
        // us
        avgLoopLatency = sum / (everyLoopLatency.length - warmupTimes) / 1000.0;
        iops = (everyLoopLatency.length - warmupTimes) / (sum / 1000.0 / 1000.0 / 1000.0);

        System.out.println("avgLoopLatency(us) " + avgLoopLatency);
        System.out.println("iops " + iops);

        sum = 0.0;
        for (int i = warmupTimes; i < everyLoopLatency.length; i++) {
            sum += Math.pow(everyLoopLatency[i] / 1000.0 - avgLoopLatency, 2);
        }
        double variance = Math.sqrt(sum / (everyLoopLatency.length - warmupTimes));
        System.out.println("variance " + variance);
    }

    void warmWrite(CrailOutputStream directStream, int operations, CrailBuffer data) throws Exception {
        while(operations-- > 0) {
            data.clear();
            ((CoreOutputStream) directStream).seek(0);

            directStream.write(data).get();
        }
    }

    void write(String filename, int size, int loop, int storageClass, int locationClass, boolean skipDir) throws Exception {
        System.out.println("write, filename " + filename + ", size " + size + ", loop " + loop + ", storageClass " + storageClass + ", locationClass " + locationClass);

        CrailBuffer buf = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(size));

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        long _bufsize = (long) size;
        int ops = 0;
        everyLoopLatency = new long[loop];

        CrailFile file = fs.create(filename, CrailNodeType.DATAFILE, CrailStorageClass.get(storageClass), CrailLocationClass.get(locationClass), !skipDir).get().asFile();
        CrailOutputStream directStream = file.getDirectOutputStream(_bufsize);
        long loopStart = 0, loopEnd = 0;

        //TODO: warmup
        warmWrite(directStream, loop, buf);

        while (ops < loop) {
            buf.clear();
            ((CoreOutputStream) directStream).seek(0);

            loopStart = System.nanoTime();
            directStream.write(buf).get();
            loopEnd = System.nanoTime();

            everyLoopLatency[ops] = loopEnd - loopStart;
            ops = ops + 1;
        }
        directStream.close();

        System.out.println("ops " + ops);
        System.out.println("final loop (us) " + ((double) (loopEnd - loopStart)) / 1000.0);
        getAvgLoopLatency();

        System.out.println("file capacity " + file.getCapacity());

        fs.getStatistics().print("close");
    }

    void writeAsync(String filename, int size, int loop, int batch, int storageClass, int locationClass, boolean skipDir) throws Exception {
        System.out.println("writeAsync, filename " + filename + ", size " + size + ", loop " + loop + ", batch " + batch + ", storageClass " + storageClass + ", locationClass " + locationClass);

        ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        for (int i = 0; i < batch; i++) {
            CrailBuffer buf = null;
            if (size == CrailConstants.BUFFER_SIZE) {
                buf = fs.allocateBuffer();
            } else if (size < CrailConstants.BUFFER_SIZE) {
                CrailBuffer _buf = fs.allocateBuffer();
                _buf.clear().limit(size);
                buf = _buf.slice();
            } else {
                buf = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(size));
            }
            bufferQueue.add(buf);
        }

        System.out.println(" debug bufferQueue length " + bufferQueue.size());

        //warmup
        warmUp(filename, warmup, bufferQueue);

        //benchmark
        System.out.println("starting benchmark...");
        LinkedBlockingQueue<Future<CrailResult>> futureQueue = new LinkedBlockingQueue<Future<CrailResult>>();
        HashMap<Integer, CrailBuffer> futureMap = new HashMap<Integer, CrailBuffer>();
        fs.getStatistics().reset();
        long _loop = (long) loop;
        long _bufsize = (long) CrailConstants.BUFFER_SIZE;
        long _capacity = _loop * _bufsize;
        double sumbytes = 0;
        double ops = 0;
        CrailFile file = fs.create(filename, CrailNodeType.DATAFILE, CrailStorageClass.get(storageClass), CrailLocationClass.get(locationClass), !skipDir).get().asFile();

        CrailOutputStream directStream = file.getDirectOutputStream(_capacity);
        long start = System.currentTimeMillis();
        for (int i = 0; i < batch - 1 && ops < loop; i++) {
            CrailBuffer buf = bufferQueue.poll();
            buf.clear();
            Future<CrailResult> future = directStream.write(buf);
            futureQueue.add(future);
            futureMap.put(future.hashCode(), buf);
            ops = ops + 1.0;

            System.out.println(" debug first for loop ops " + ops + " futureQueue size " + futureQueue.size());
        }
        while (ops < loop) {
            CrailBuffer buf = bufferQueue.poll();
            buf.clear();
            Future<CrailResult> future = directStream.write(buf);
            futureQueue.add(future);
            futureMap.put(future.hashCode(), buf);

            System.out.println(" debug second for loop futureQueue size " + futureQueue.size());

            future = futureQueue.poll();
            future.get();
            buf = futureMap.get(future.hashCode());
            bufferQueue.add(buf);

            sumbytes = sumbytes + buf.capacity();
            ops = ops + 1.0;

            System.out.println(" debug second for loop ops " + ops + " futureQueue size " + futureQueue.size());
        }
        while (!futureQueue.isEmpty()) {
            Future<CrailResult> future = futureQueue.poll();
            future.get();
            CrailBuffer buf = futureMap.get(future.hashCode());
            sumbytes = sumbytes + buf.capacity();
            ops = ops + 1.0;
        }
        long end = System.currentTimeMillis();
        double executionTime = ((double) (end - start)) / 1000.0;
        double throughput = 0.0;
        double latency = 0.0;
        double sumbits = sumbytes * 8.0;
        if (executionTime > 0) {
            throughput = sumbits / executionTime / 1000.0 / 1000.0;
            latency = 1000000.0 * executionTime / ops;
        }
        directStream.close();

        System.out.println("execution time " + executionTime);
        System.out.println("ops " + ops);
        System.out.println("sumbytes " + sumbytes);
        System.out.println("throughput " + throughput);
        System.out.println("latency " + latency);

        fs.getStatistics().print("close");
    }

    void testNetworkLatency(String filename, int size, int loop) throws Exception {
        System.out.println("testNetworkLatency, filename " + filename + ", size " + size + ", loop " + loop);
        int NumDataBlock = CrailConstants.ERASURECODING_K;
        int NumParityBlock = CrailConstants.ERASURECODING_M;
        int Microec_buffer_size = size / NumDataBlock;

        CrailConstants.REPLICATION_FACTOR = NumDataBlock + NumParityBlock - 1;
        System.out.println("CrailConstants.REPLICATION_FACTOR has changed to " + CrailConstants.REPLICATION_FACTOR);

        ByteBuffer[] data = new ByteBuffer[NumDataBlock + NumParityBlock];
        ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        allocateDataBuffers(data, Microec_buffer_size, dataBufferQueue);

        long _bufsize = (long) Microec_buffer_size;
        int ops = 0;
        everyLoopLatency = new long[loop];

        CrailFile file = fs.createReplicasFile(filename, CrailNodeType.DATAFILE, CrailStorageClass.get(0), CrailLocationClass.get(0), true, RedundancyType.ERASURECODING_TYPE).get().asFile();
        CrailOutputStream directStream = ((ReplicasFile) file).getReplicasDirectOutputStream(_bufsize);

        getRedundantFileCapacity(file);

        long loopStart = 0, loopEnd = 0;
        while (ops < loop) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            ((CoreReplicasOutputStream) directStream).seek(0);

//            loopStart = System.nanoTime();
            ((CoreReplicasOutputStream) directStream).writeReplicasAsync(dataBufferQueue);
            loopStart = System.nanoTime();
            ((CoreReplicasOutputStream) directStream).syncResults();
            loopEnd = System.nanoTime();

            everyLoopLatency[ops] = loopEnd - loopStart;
            ops += 1;
        }
        directStream.close();

        getAvgLoopLatency();

        getRedundantFileCapacity(file);

        fs.getStatistics().print("close");
    }

    void testDataNetworkLatency(String filename, int size, int loop) throws Exception {
        System.out.println("testDataNetworkLatency, filename " + filename + ", size " + size + ", loop " + loop);
        int NumDataBlock = CrailConstants.ERASURECODING_K;
        int NumParityBlock = CrailConstants.ERASURECODING_M;
        int Microec_buffer_size = size / NumDataBlock;

        CrailConstants.REPLICATION_FACTOR = NumDataBlock + NumParityBlock - 1;
        System.out.println("CrailConstants.REPLICATION_FACTOR has changed to " + CrailConstants.REPLICATION_FACTOR);

        ByteBuffer[] data = new ByteBuffer[NumDataBlock];
        ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        allocateDataBuffers(data, Microec_buffer_size, dataBufferQueue);

        long _bufsize = (long) Microec_buffer_size;
        int ops = 0;
        everyLoopLatency = new long[loop];

        CrailFile file = fs.createReplicasFile(filename, CrailNodeType.DATAFILE, CrailStorageClass.get(0), CrailLocationClass.get(0), true, RedundancyType.ERASURECODING_TYPE).get().asFile();
        CrailOutputStream directStream = ((ReplicasFile) file).getReplicasDirectOutputStream(_bufsize);

        getRedundantFileCapacity(file);

        long loopStart = 0, loopEnd = 0;
        while (ops < loop) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            ((CoreReplicasOutputStream) directStream).seek(0);

//            loopStart = System.nanoTime();
            ((CoreReplicasOutputStream) directStream).writeReplicasAsync(dataBufferQueue);
            loopStart = System.nanoTime();
            ((CoreReplicasOutputStream) directStream).syncResults();
            loopEnd = System.nanoTime();

            everyLoopLatency[ops] = loopEnd - loopStart;
            ops += 1;
        }
        directStream.close();

        getAvgLoopLatency();

        getRedundantFileCapacity(file);

        fs.getStatistics().print("close");
    }

    void testParityNetworkLatency(String filename, int size, int loop) throws Exception {
        System.out.println("testParityNetworkLatency, filename " + filename + ", size " + size + ", loop " + loop);
        int NumDataBlock = CrailConstants.ERASURECODING_K;
        int NumParityBlock = CrailConstants.ERASURECODING_M;
        int Microec_buffer_size = size / NumDataBlock;

        CrailConstants.REPLICATION_FACTOR = NumDataBlock + NumParityBlock - 1;
        System.out.println("CrailConstants.REPLICATION_FACTOR has changed to " + CrailConstants.REPLICATION_FACTOR);

        ByteBuffer[] data = new ByteBuffer[NumParityBlock];
        ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        allocateDataBuffers(data, Microec_buffer_size, dataBufferQueue);

        long _bufsize = (long) Microec_buffer_size;
        int ops = 0;
        everyLoopLatency = new long[loop];

        CrailFile file = fs.createReplicasFile(filename, CrailNodeType.DATAFILE, CrailStorageClass.get(0), CrailLocationClass.get(0), true, RedundancyType.ERASURECODING_TYPE).get().asFile();
        CrailOutputStream directStream = ((ReplicasFile) file).getReplicasDirectOutputStream(_bufsize);

        getRedundantFileCapacity(file);

        long loopStart = 0, loopEnd = 0;
        while (ops < loop) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            ((CoreReplicasOutputStream) directStream).seek(0);

//            loopStart = System.nanoTime();
            ((CoreReplicasOutputStream) directStream).writeParitySplitsAsync(dataBufferQueue, NumDataBlock, NumParityBlock);
            loopStart = System.nanoTime();
            ((CoreReplicasOutputStream) directStream).syncResults();
            loopEnd = System.nanoTime();

            everyLoopLatency[ops] = loopEnd - loopStart;
            ops += 1;
        }
        directStream.close();

        getAvgLoopLatency();

        getRedundantFileCapacity(file);

        fs.getStatistics().print("close");
    }

    void warmupWriteReplicas(CrailOutputStream directStream, int operations, CrailBuffer data, ConcurrentLinkedQueue<CrailBuffer> transferQueue) throws Exception {
        while (operations-- > 0) {
            data.clear();
            ((CoreReplicasOutputStream) directStream).clearLatencyLogger();
            ((CoreReplicasOutputStream) directStream).seek(0);

            transferQueue.clear();
            transferQueue.add(data);
            for (int i = 0; i < CrailConstants.REPLICATION_FACTOR; i++) {
                transferQueue.add(data.slice());
            }

            ((CoreReplicasOutputStream) directStream).writeReplicasAsync(transferQueue);
            ((CoreReplicasOutputStream) directStream).syncResults();
        }
    }

    CrailFile writeReplicas(String filename, int size, int loop, int storageClass, int locationClass, boolean skipDir) throws Exception {
        System.out.println("writeReplicas, filename " + filename + ", size " + size + ", loop " + loop + ", storageClass " + storageClass + ", locationClass " + locationClass);

        CrailBuffer data = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(size));
        ConcurrentLinkedQueue<CrailBuffer> transferQueue = new ConcurrentLinkedQueue<CrailBuffer>();

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        long _bufsize = (long) size;
        int ops = 0;
        everyLoopLatency = new long[loop];

        CrailFile file = fs.createReplicasFile(filename, CrailNodeType.DATAFILE, CrailStorageClass.get(storageClass), CrailLocationClass.get(locationClass), !skipDir, RedundancyType.REPLICAS_TYPE).get().asFile();
        CrailOutputStream directStream = ((ReplicasFile) file).getReplicasDirectOutputStream(_bufsize);
        long loopStart = 0, loopEnd = 0;

        warmupWriteReplicas(directStream, loop, data, transferQueue);

        while (ops < loop) {
            data.clear();
            ((CoreReplicasOutputStream) directStream).clearLatencyLogger();
            ((CoreReplicasOutputStream) directStream).seek(0);

            // change data of data buffer
            setCrailBuffer(data, getRandomString(size));

            loopStart = System.nanoTime();
            transferQueue.clear();
            transferQueue.add(data);
            for (int i = 0; i < CrailConstants.REPLICATION_FACTOR; i++) {
                transferQueue.add(data.slice());
            }

            ((CoreReplicasOutputStream) directStream).writeReplicasAsync(transferQueue);
            ((CoreReplicasOutputStream) directStream).syncResults();

            loopEnd = System.nanoTime();
            everyLoopLatency[ops] = loopEnd - loopStart;
            ops += 1;
        }
        directStream.close();

        System.out.println("ops " + ops);
        System.out.println("final loop (us) " + ((double) (loopEnd - loopStart)) / 1000.0);

        getAvgLoopLatency();

        getRedundantFileCapacity(file);

        fs.getStatistics().print("close");

        return file;
    }


    // Encoding then pipeline network transfering
    CrailFile writeECPipeline(String filename, int size, int encodingSplitSize, int loop, int storageClass, int locationClass, int pureMonECSubStripeNum, boolean skipDir, boolean isPureMonEC) throws Exception {
        System.out.println("writeECPipeline, filename " + filename + ", size " + size + ", encodingSplitSize " + encodingSplitSize + ", loop " + loop + ", storageClass " + storageClass + ", locationClass " + locationClass + ", pureMonECSubStripeNum " + pureMonECSubStripeNum + ", isPureMonEC " + isPureMonEC);

        // simulate object split
        int NumDataBlock = CrailConstants.ERASURECODING_K;
        int NumParityBlock = CrailConstants.ERASURECODING_M;
        Crailcoding codingLib = new Crailcoding(NumDataBlock, NumParityBlock);
        CodingCoreCache coreCache = new CodingCoreCache();
        int blockSize = size / NumDataBlock;
        int bindCoreIdx = 11;
        if (encodingSplitSize > blockSize) {
            encodingSplitSize = blockSize;
            System.out.println("encodingSplitSize has changed to " + encodingSplitSize);
        }
        if (size % (encodingSplitSize * NumDataBlock) != 0) {
            throw new Exception("Object size must be an integer multiple of (encodingSplitSize * NumDataBlock)!");
        }
        int networkRound = size / (encodingSplitSize * NumDataBlock);

        ByteBuffer[] input = new ByteBuffer[NumDataBlock];
        ByteBuffer[] output = new ByteBuffer[NumParityBlock];
        ByteBuffer[] subInput = new ByteBuffer[NumDataBlock];
        ByteBuffer[] subOutput = new ByteBuffer[NumParityBlock];
        ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        ConcurrentLinkedQueue<CrailBuffer> parityBufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        ConcurrentLinkedQueue<CrailBuffer> transferQueue = new ConcurrentLinkedQueue<>();

        allocateDataBuffers(input, blockSize, dataBufferQueue);
        allocateParityBuffers(output, blockSize, parityBufferQueue);

        // states of time log
        long syncStart, syncEnd;
        long loopStart = 0, loopEnd = 0;
        long[] subTransferSubmitStart = new long[networkRound];
        long[] subTransferSubmitEnd = new long[networkRound];
        long[] subCodingStart = new long[networkRound];
        long[] subCodingEnd = new long[networkRound];
        int[] subUsedCore = new int[networkRound];
        List<List<Long>> loopLatencyRecord = new ArrayList<>();
        for (int i = 0; i < loop; i++) {
            loopLatencyRecord.add(new ArrayList<>());
        }

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        long _bufsize = (long) blockSize;
        int ops = 0;
        everyLoopLatency = new long[loop];

        // EC file can be processed in the same way with Replicas file
        CrailConstants.REPLICATION_FACTOR = NumDataBlock + NumParityBlock - 1;
        System.out.println("CrailConstants.REPLICATION_FACTOR has changed to " + CrailConstants.REPLICATION_FACTOR);

        CrailFile file = fs.createReplicasFile(filename, CrailNodeType.DATAFILE, CrailStorageClass.get(storageClass), CrailLocationClass.get(locationClass), !skipDir, RedundancyType.ERASURECODING_TYPE).get().asFile();
        CrailOutputStream directStream = ((ReplicasFile) file).getReplicasDirectOutputStream(_bufsize);
        getRedundantFileCapacity(file);

        warmupECPipeline(directStream, loop, coreCache, codingLib, subInput, subOutput, dataBufferQueue, parityBufferQueue, transferQueue, networkRound, encodingSplitSize, isPureMonEC, pureMonECSubStripeNum);

        while (ops < loop) {

            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            clearBufferOfConcurrentLinkedQueue(parityBufferQueue);
            ((CoreReplicasOutputStream) directStream).seek(0);
            ((CoreReplicasOutputStream) directStream).clearLatencyLogger();

            // change data of data buffer
            randomDataBuffers(input, blockSize);

            loopStart = System.nanoTime();
//            bindCoreIdx = coreCache.getFreeCoreIdx();
//            bindCoreIdx = coreCache.randomGetFreeCoreIdx();
            bindCoreIdx = size > maxSmallObjSize ? coreCache.randomGetFreeCoreIdx() : coreCache.getFreeCoreIdx();
            for (int i = 0; i < networkRound; i++) {
                transferQueue.clear();

                int j = 0;
                for (CrailBuffer buf : dataBufferQueue) {
                    buf.limit((i + 1) * encodingSplitSize);
                    buf.position(i * encodingSplitSize);

                    CrailBuffer sbuf = buf.slice();
                    transferQueue.add(sbuf);
                    subInput[j++] = sbuf.getByteBuffer();
                }

                j = 0;
                for (CrailBuffer buf : parityBufferQueue) {
                    buf.limit((i + 1) * encodingSplitSize);
                    buf.position(i * encodingSplitSize);

                    CrailBuffer sbuf = buf.slice();
                    transferQueue.add(sbuf);
                    subOutput[j++] = sbuf.getByteBuffer();
                }

                subCodingStart[i] = System.nanoTime();
//                bindCoreIdx = coreCache.randomGetFreeCoreIdx();
//                bindCoreIdx = size > maxSmallObjSize ? coreCache.randomGetFreeCoreIdx() : coreCache.getFreeCoreIdx();
                subUsedCore[i] = bindCoreIdx;
                // Encoding
                if (isPureMonEC) {
                    codingLib.PureMicroecEncoding(subInput, subOutput, pureMonECSubStripeNum, NumDataBlock, NumParityBlock, encodingSplitSize, bindCoreIdx);
                } else {
                    codingLib.NativeEncoding(subInput, subOutput, NumDataBlock, NumParityBlock, encodingSplitSize, bindCoreIdx);
                }
//                coreCache.releaseCore();
                subCodingEnd[i] = System.nanoTime();

                subTransferSubmitStart[i] = System.nanoTime();
                ((CoreReplicasOutputStream) directStream).writeReplicasAsync(transferQueue);
                subTransferSubmitEnd[i] = System.nanoTime();
            }
            coreCache.releaseCore();

            syncStart = System.nanoTime();
            // sync all writes
            ((CoreReplicasOutputStream) directStream).syncResults();
            syncEnd = System.nanoTime();

            loopEnd = System.nanoTime();
            everyLoopLatency[(int) ops] = loopEnd - loopStart;

            loopLatencyRecord.get(ops).add(loopEnd - loopStart);
            for (int i = 0; i < networkRound; i++) {
                loopLatencyRecord.get(ops).add((long) subUsedCore[i]);
                loopLatencyRecord.get(ops).add(subCodingEnd[i] - subCodingStart[i]);
                loopLatencyRecord.get(ops).add(subTransferSubmitEnd[i] - subTransferSubmitStart[i]);
            }
            loopLatencyRecord.get(ops).add(syncEnd - syncStart);

            ops = ops + 1;
        }

        directStream.close();

        System.out.println("ops " + ops);
        System.out.println("final loop (us) " + ((double) (loopEnd - loopStart)) / 1000.0);

        getAvgLoopLatency();

        for (int i = 0; i < loopLatencyRecord.size(); i++) {
            System.out.println("Loop " + i + " " + loopLatencyRecord.get(i));
        }

        getRedundantFileCapacity(file);

        fs.getStatistics().print("close");

        return file;
    }


    void setByteBuffer(ByteBuffer buffer, String cs) {
        buffer.clear();
        buffer.put(cs.getBytes());
        buffer.clear();
    }

    void setCrailBuffer(CrailBuffer buffer, String cs) {
        buffer.clear();
        buffer.put(cs.getBytes());
        buffer.clear();
    }

    void warmupHydra(CrailOutputStream directStream, int operations, int blockSize, ByteBuffer[] input, ByteBuffer[] output, ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue, ConcurrentLinkedQueue<CrailBuffer> parityBufferQueue, Crailcoding codingLib, CodingCoreCache coreCache) throws Exception {
        int bindCoreIdx = 11;
        int NumDataBlock = input.length, NumParityBlock = output.length;
        int ops = 0;
        while(ops++ < operations) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            clearBufferOfConcurrentLinkedQueue(parityBufferQueue);
            ((CoreReplicasOutputStream) directStream).clearLatencyLogger();
            ((CoreReplicasOutputStream) directStream).seek(0);

            // change data of data buffer
//            randomDataBuffers(input, blockSize);
            ((CoreReplicasOutputStream) directStream).writeReplicasAsync(dataBufferQueue);

            bindCoreIdx = coreCache.randomGetFreeCoreIdx();
            codingLib.NativeEncoding(input, output, NumDataBlock, NumParityBlock, blockSize, bindCoreIdx);

            ((CoreReplicasOutputStream) directStream).writeParitySplitsAsync(parityBufferQueue, NumDataBlock, NumParityBlock);

            ((CoreReplicasOutputStream) directStream).syncResults();

            // release core idx
            coreCache.releaseCore();
        }
    }

    CrailFile writeHydra(String filename, int size, int loop, int storageClass, int locationClass, boolean skipDir) throws Exception {
        System.out.println("writeHydra, filename " + filename + ", size " + size + ", loop " + loop + ", storageClass " + storageClass + ", locationClass " + locationClass);

        // simulate object split
        int NumDataBlock = CrailConstants.ERASURECODING_K;
        int NumParityBlock = CrailConstants.ERASURECODING_M;
        // EC file can be processed in the same way with Replicas file
        CrailConstants.REPLICATION_FACTOR = NumDataBlock + NumParityBlock - 1;
        System.out.println("CrailConstants.REPLICATION_FACTOR has changed to " + CrailConstants.REPLICATION_FACTOR);

        Crailcoding codingLib = new Crailcoding(NumDataBlock, NumParityBlock);
        // pure ec split size
        int blockSize = size / NumDataBlock;

        ByteBuffer[] input = new ByteBuffer[NumDataBlock];
        ByteBuffer[] output = new ByteBuffer[NumParityBlock];
        ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<CrailBuffer> parityBufferQueue = new ConcurrentLinkedQueue<>();

        allocateDataBuffers(input, blockSize, dataBufferQueue);
        allocateParityBuffers(output, blockSize, parityBufferQueue);

        CodingCoreCache coreCache = new CodingCoreCache();
        int bindCoreIdx = 11;

        // states of time log
        long codingSubmitStart, codingSubmitEnd, dataSubmitStart, dataSubmitEnd, paritySubmitStart, paritySubmitEnd, syncStart, syncEnd;
        long loopStart = 0, loopEnd = 0;
        List<List<Long>> loopLatencyRecord = new ArrayList<>();
        for (int i = 0; i < loop; i++) {
            loopLatencyRecord.add(new ArrayList<>());
        }
        int ops = 0;
        everyLoopLatency = new long[loop];

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        long _bufsize = (long) blockSize;

        CrailFile file = fs.createReplicasFile(filename, CrailNodeType.DATAFILE, CrailStorageClass.get(storageClass), CrailLocationClass.get(locationClass), !skipDir, RedundancyType.ERASURECODING_TYPE).get().asFile();
        CrailOutputStream directStream = ((ReplicasFile) file).getReplicasDirectOutputStream(_bufsize);
        getRedundantFileCapacity(file);

        warmupHydra(directStream, loop, blockSize, input, output, dataBufferQueue, parityBufferQueue, codingLib, coreCache);
        while (ops < loop) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            clearBufferOfConcurrentLinkedQueue(parityBufferQueue);
            ((CoreReplicasOutputStream) directStream).clearLatencyLogger();
            ((CoreReplicasOutputStream) directStream).seek(0);

            // change data of data buffer
            randomDataBuffers(input, blockSize);

            loopStart = System.nanoTime();
            dataSubmitStart = System.nanoTime();
            // async send all data blocks
            ((CoreReplicasOutputStream) directStream).writeReplicasAsync(dataBufferQueue);
            dataSubmitEnd = System.nanoTime();

            codingSubmitStart = System.nanoTime();
            // set codingTask (coding buffer + core idx)
//            bindCoreIdx = coreCache.getFreeCoreIdx();
//            bindCoreIdx = coreCache.randomGetFreeCoreIdx();
            bindCoreIdx = size > maxSmallObjSize ? coreCache.randomGetFreeCoreIdx() : coreCache.getFreeCoreIdx();
//            System.out.println("bindCoreIdx "+bindCoreIdx);
            codingLib.NativeEncoding(input, output, NumDataBlock, NumParityBlock, blockSize, bindCoreIdx);
            codingSubmitEnd = System.nanoTime();

            paritySubmitStart = System.nanoTime();
            ((CoreReplicasOutputStream) directStream).writeParitySplitsAsync(parityBufferQueue, NumDataBlock, NumParityBlock);
            paritySubmitEnd = System.nanoTime();

            syncStart = System.nanoTime();
            // sync all writes
            ((CoreReplicasOutputStream) directStream).syncResults();
            syncEnd = System.nanoTime();

            // release core idx
            coreCache.releaseCore();
            loopEnd = System.nanoTime();

            everyLoopLatency[ops] = loopEnd - loopStart;
            loopLatencyRecord.get(ops).add(loopEnd - loopStart);
            loopLatencyRecord.get(ops).add((long) bindCoreIdx);
            loopLatencyRecord.get(ops).add(codingSubmitEnd - codingSubmitStart);
            loopLatencyRecord.get(ops).add(dataSubmitEnd - dataSubmitStart);
            loopLatencyRecord.get(ops).add(paritySubmitEnd - paritySubmitStart);
//            loopLatencyRecord.get(ops).addAll(((CoreReplicasOutputStream) directStream).getLatencyLogger());
            loopLatencyRecord.get(ops).add(syncEnd - syncStart);

            ops += 1;
        }

        directStream.close();

        System.out.println("ops " + ops);
        System.out.println("final loop (us) " + ((double) (loopEnd - loopStart)) / 1000.0);

        getAvgLoopLatency();

        for (int i = 0; i < loopLatencyRecord.size(); i++) {
            System.out.println("Loop " + i + " " + loopLatencyRecord.get(i));
        }

        getRedundantFileCapacity(file);

        fs.getStatistics().print("close");

        return file;
    }

    CrailFile writePipelineHydra(String filename, int size, int transferSize, int loop, int storageClass, int locationClass, int monECSubStripeNum, boolean skipDir) throws Exception {
        System.out.println("writePipelineHydra, filename " + filename + ", size " + size + ", transferSize " + transferSize + ", loop " + loop + ", storageClass " + storageClass + ", locationClass " + locationClass + ", monECSubStripeNum " + monECSubStripeNum);

        // simulate object split
        int NumDataBlock = CrailConstants.ERASURECODING_K;
        int NumParityBlock = CrailConstants.ERASURECODING_M;
        // EC file can be processed in the same way with Replicas file
        CrailConstants.REPLICATION_FACTOR = NumDataBlock + NumParityBlock - 1;
        System.out.println("CrailConstants.REPLICATION_FACTOR has changed to " + CrailConstants.REPLICATION_FACTOR);

        Crailcoding codingLib = new Crailcoding(NumDataBlock, NumParityBlock);
        // pure ec split size
        int Microec_buffer_size = size / NumDataBlock;
        // monec encoding size
        int Microec_buffer_split_size = Microec_buffer_size / monECSubStripeNum;

        if(transferSize > Microec_buffer_size){
            transferSize = Microec_buffer_size;
            System.out.println("transferSize has changed to " + transferSize);
        }

        List<Integer> networkSizeArray = getFixedNetworkSize(Microec_buffer_size, transferSize);
        System.out.println("networkSizeArray " + networkSizeArray);
        int networkRound = networkSizeArray.size();

        ByteBuffer[] input = new ByteBuffer[NumDataBlock];
        ByteBuffer[] output = new ByteBuffer[NumParityBlock];
        ByteBuffer cursor = ByteBuffer.allocateDirect(monECSubStripeNum);
        String fullzero = getFullZeroString(monECSubStripeNum);
        ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<CrailBuffer> parityBufferQueue = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<CrailBuffer> transferQueue = new ConcurrentLinkedQueue<>();

        allocateDataBuffers(input, Microec_buffer_size, dataBufferQueue);
        allocateParityBuffers(output, Microec_buffer_size, parityBufferQueue);

        CodingCoreCache coreCache = new CodingCoreCache();
        int bindCoreIdx = 11;
        EncodingTask encodingTask = new EncodingTask(codingLib, monECSubStripeNum, NumDataBlock, NumParityBlock, Microec_buffer_size);
        ExecutorService es = Executors.newSingleThreadExecutor();

        // states of time log
        long codingSubmitStart, codingSubmitEnd, dataSubmitStart, dataSubmitEnd, paritySubmitStart, paritySubmitEnd, syncStart, syncEnd;
        long loopStart = 0, loopEnd = 0;
        long[] subParitySubmitStart = new long[networkRound];
        long[] subParitySubmitEnd = new long[networkRound];
        long[] subParityInLoopStart = new long[networkRound];
        long[] subParityInLoopEnd = new long[networkRound];
        long[] subPackingStart = new long[networkRound];
        long[] subPackingEnd = new long[networkRound];
        List<List<Long>> loopLatencyRecord = new ArrayList<>();
        List<List<Long>> loopTimestampRecord = new ArrayList<>();
        for (int i = 0; i < loop; i++) {
            loopLatencyRecord.add(new ArrayList<>());
            loopTimestampRecord.add(new ArrayList<>());
        }
        int ops = 0;
        everyLoopLatency = new long[loop];
        // Clear data and parity buffer before encoding (done in warmupMicroEC)
        // clear recorders' content
        Arrays.fill(subParitySubmitStart, 0);

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        long _bufsize = (long) Microec_buffer_size;

        CrailFile file = fs.createReplicasFile(filename, CrailNodeType.DATAFILE, CrailStorageClass.get(storageClass), CrailLocationClass.get(locationClass), !skipDir, RedundancyType.ERASURECODING_TYPE).get().asFile();
        CrailOutputStream directStream = ((ReplicasFile) file).getReplicasDirectOutputStream(_bufsize);
        getRedundantFileCapacity(file);

        // warm phrase
        // Goal: warm up data structures and function calls
        warmupMicroEC(directStream, loop, es, coreCache, encodingTask, input, output, dataBufferQueue, parityBufferQueue, transferQueue, cursor, networkSizeArray);

        while (ops < loop) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            clearBufferOfConcurrentLinkedQueue(parityBufferQueue);
            setByteBuffer(cursor, fullzero);
            ((CoreReplicasOutputStream) directStream).clearLatencyLogger();
            Arrays.fill(subParitySubmitStart, 0);
            ((CoreReplicasOutputStream) directStream).seek(0);

            // change data of data buffer
            randomDataBuffers(input, Microec_buffer_size);

//            if(ops == loop - 1000){
//                callShellByExec("/home/hadoop/testCrail/clear.sh");
//                callShellByExec("/home/hadoop/testCrail/usage.sh");
//            }

            loopStart = System.nanoTime();

            dataSubmitStart = System.nanoTime();
            // async send all data blocks
            ((CoreReplicasOutputStream) directStream).writeReplicasAsync(dataBufferQueue);
            dataSubmitEnd = System.nanoTime();

            codingSubmitStart = System.nanoTime();
            // set EncodingCodingTask (coding buffer + core idx)
//            bindCoreIdx = coreCache.getFreeCoreIdx();
//            bindCoreIdx = coreCache.randomGetFreeCoreIdx();
            bindCoreIdx = size > maxSmallObjSize ? coreCache.randomGetFreeCoreIdx() : coreCache.getFreeCoreIdx();
//            System.out.println("bindCoreIdx "+bindCoreIdx);
            encodingTask.setEncodingBuffer(input, output, cursor);
            encodingTask.setBindCoreidx(bindCoreIdx);
            // start encoding based on the size of the objects
            // block size of an object <= minBlockSize: sync coding
            // block size of an object > minBlockSize: async coding
            if (Microec_buffer_size <= minBlockSize || networkRound == 1) {
                codingLib.MicroecEncoding(input, output, cursor, monECSubStripeNum, NumDataBlock, NumParityBlock, Microec_buffer_size, bindCoreIdx);
            } else {
                es.submit(encodingTask);
            }
            codingSubmitEnd = System.nanoTime();

            paritySubmitStart = System.nanoTime();
            int networkIndex = 1;
            while (networkIndex <= networkRound) {
                int tmp = networkSizeArray.get(networkIndex - 1) / Microec_buffer_split_size - 1;

                if (subParitySubmitStart[networkIndex - 1] == 0) {
                    subParitySubmitStart[networkIndex - 1] = System.nanoTime();
                }

                if (cursor.get(tmp) == '1') {
                    subParityInLoopStart[networkIndex - 1] = System.nanoTime();
                    transferQueue.clear();
//                    subParityInLoopEnd[networkIndex-1]=System.nanoTime();
                    subPackingStart[networkIndex - 1] = System.nanoTime();
                    for (CrailBuffer buf : parityBufferQueue) {
                        buf.limit(networkSizeArray.get(networkIndex - 1));
                        if (networkIndex == 1) {
                            buf.position(0);
                        } else {
                            buf.position(networkSizeArray.get(networkIndex - 2));
                        }

                        CrailBuffer sbuf = buf.slice();
                        transferQueue.add(sbuf);
                    }
                    subPackingEnd[networkIndex - 1] = System.nanoTime();
//                    subParityInLoopEnd[networkIndex-1]=System.nanoTime();

//                    subParityInLoopStart[networkIndex-1]=System.nanoTime();
                    ((CoreReplicasOutputStream) directStream).writeParitySplitsAsync(transferQueue, NumDataBlock, NumParityBlock);
                    subParitySubmitEnd[networkIndex - 1] = System.nanoTime();
                    networkIndex++;
                }
            }
            paritySubmitEnd = System.nanoTime();

            syncStart = System.nanoTime();
            // sync all writes
            ((CoreReplicasOutputStream) directStream).syncResults();
            syncEnd = System.nanoTime();

            // release core idx
            coreCache.releaseCore();

            loopEnd = System.nanoTime();

//            if(ops == loop - 1){
//                callShellByExec("/home/hadoop/testCrail/usage.sh");
//            }

            everyLoopLatency[ops] = loopEnd - loopStart;
            loopLatencyRecord.get(ops).add(loopEnd - loopStart);
            loopLatencyRecord.get(ops).add((long) bindCoreIdx);
            loopLatencyRecord.get(ops).add(codingSubmitEnd - codingSubmitStart);
            loopLatencyRecord.get(ops).add(dataSubmitEnd - dataSubmitStart);
            loopLatencyRecord.get(ops).add(paritySubmitEnd - paritySubmitStart);
            for (int i = 0; i < networkRound; i++) {
//                loopLatencyRecord.get(ops).add(subParitySubmitEnd[i]-subParitySubmitStart[i]);
                loopLatencyRecord.get(ops).add(subParitySubmitEnd[i] - subParityInLoopStart[i]);
//                loopLatencyRecord.get(ops).add(subParityInLoopEnd[i]-subParityInLoopStart[i]);
                loopLatencyRecord.get(ops).add(subPackingEnd[i] - subPackingStart[i]);
            }
//            loopLatencyRecord.get(ops).addAll(((CoreReplicasOutputStream) directStream).getLatencyLogger());
            loopLatencyRecord.get(ops).add(syncEnd - syncStart);

            // save timestamp
            loopTimestampRecord.get(ops).add(loopStart);
            loopTimestampRecord.get(ops).add(dataSubmitStart);
            loopTimestampRecord.get(ops).add(dataSubmitEnd);
            loopTimestampRecord.get(ops).add(codingSubmitStart);
            loopTimestampRecord.get(ops).add(codingSubmitEnd);
            loopTimestampRecord.get(ops).add(paritySubmitStart);
            for (int i = 0; i < networkRound; i++) {
                loopTimestampRecord.get(ops).add(subParitySubmitStart[i]);
                loopTimestampRecord.get(ops).add(subParityInLoopStart[i]);
                loopTimestampRecord.get(ops).add(subPackingStart[i]);
                loopTimestampRecord.get(ops).add(subPackingEnd[i]);
                loopTimestampRecord.get(ops).add(subParitySubmitEnd[i]);
            }
            loopTimestampRecord.get(ops).add(paritySubmitEnd);
            loopTimestampRecord.get(ops).add(syncStart);
            loopTimestampRecord.get(ops).add(syncEnd);
            loopTimestampRecord.get(ops).add(loopEnd);

            ops += 1;
        }

        es.shutdown();
        directStream.close();

        System.out.println("ops " + ops);
        System.out.println("final loop (us) " + ((double) (loopEnd - loopStart)) / 1000.0);

        getAvgLoopLatency();

        System.out.println("Breakdown info");
        for (int i = 0; i < loopLatencyRecord.size(); i++) {
            System.out.println("Loop " + i + " " + loopLatencyRecord.get(i));
        }

        System.out.println("Timestamp info");
        for (int i = 0; i < loopTimestampRecord.size(); i++) {
            System.out.println("Timestamp " + i + " " + loopTimestampRecord.get(i));
        }

        getRedundantFileCapacity(file);

        fs.getStatistics().print("close");

        return file;
    }

    // create a full-zero string
    String getFullZeroString(int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append("0");
        }
        return sb.toString();
    }

    void randomDataBuffers(ByteBuffer[] arr, int bufferSize) throws IOException {
        for (int i = 0; i < arr.length; i++) {
            setByteBuffer(arr[i], getRandomString(bufferSize));
        }
    }

    // allocate read buffer
    void allocateDataBuffers(ConcurrentLinkedQueue<CrailBuffer> queue, int bufferSize) throws IOException {
        for (int i = 0; i < CrailConstants.ERASURECODING_K; i++) {
            queue.add(OffHeapBuffer.wrap(ByteBuffer.allocateDirect(bufferSize)));
        }
    }

    void allocateDataBuffers(ByteBuffer[] arr, int bufferSize, ConcurrentLinkedQueue<CrailBuffer> queue) throws IOException {
        for (int i = 0; i < arr.length; i++) {
            arr[i] = ByteBuffer.allocateDirect(bufferSize);
            setByteBuffer(arr[i], getRandomString(bufferSize));
            queue.add(OffHeapBuffer.wrap(arr[i]));
        }
    }

    void allocateDataBuffers(ByteBuffer[] arr, int bufferSize) throws IOException {
        for (int i = 0; i < arr.length; i++) {
            arr[i] = ByteBuffer.allocateDirect(bufferSize);
            setByteBuffer(arr[i], getRandomString(bufferSize));
        }
    }

    void allocateParityBuffers(ByteBuffer[] arr, int bufferSize, ConcurrentLinkedQueue<CrailBuffer> queue) throws IOException {
        for (int i = 0; i < arr.length; i++) {
            arr[i] = ByteBuffer.allocateDirect(bufferSize);
            arr[i].clear();
            queue.add(OffHeapBuffer.wrap(arr[i]));
        }
    }

    void allocateParityBuffers(ByteBuffer[] arr, int bufferSize) throws IOException {
        for (int i = 0; i < arr.length; i++) {
            arr[i] = ByteBuffer.allocateDirect(bufferSize);
            arr[i].clear();
        }
    }

    // clear index of a given ConcurrentLinkedQueue
    void clearBufferOfConcurrentLinkedQueue(ConcurrentLinkedQueue<CrailBuffer> q) {
        for (CrailBuffer buf : q) {
            buf.clear();
        }
    }

    // clear index of a given ByteBuffer array
    void clearBufferOfArray(ByteBuffer[] arr) {
        for (ByteBuffer buf : arr) {
            buf.clear();
        }
    }

    // print redundant file location
    void getRedundantFileCapacity(CrailFile file) {
        CrailFile[] slaveFiles = ((ReplicasFile) file).getSlavesFile();
        System.out.println("replicas primary file " + file.getPath() + " capacity " + file.getCapacity());
        for (int i = 0; i < slaveFiles.length; i++) {
            System.out.println("replicas slave file " + slaveFiles[i].getPath() + " capacity " + slaveFiles[i].getCapacity());
        }
    }

    void getRedundantFileCapacity(List<CrailFile> files) {
        for (CrailFile file : files) {
            System.out.println("recovery file " + file.getPath() + " capacity " + file.getCapacity());
        }
    }

    // print redundant file distribution
    void getRedundantFileDistribution(CrailFile file) throws Exception {
        System.out.println("replicas primary file " + file.getPath() + " capacity " + file.getCapacity());
        List<List<CrailBlockLocation>> blockLocations = ((ReplicasFile) file).getSlavesBlockLocations(0, file.getCapacity());
        for (List<CrailBlockLocation> fileList : blockLocations) {
            for (CrailBlockLocation cb : fileList) {
                for (String h : cb.getHosts()) {
                    System.out.println(h);
                }
            }
        }
    }

    List<Integer> getNetworkSizeArray(int blockSize) {
        List<Integer> networkSizeArray = new ArrayList<>();
        int incremental = blockSize / 2;
        int networkSize = incremental;

        while (incremental >= minBlockSize) {
            networkSizeArray.add(networkSize);
            networkSize += incremental / 2;
            incremental /= 2;
        }

        networkSizeArray.add(blockSize);

        return networkSizeArray;
    }

    List<Integer> getFixedNetworkSize(int blockSize, int networkSize) {
        List<Integer> networkSizeArray = new ArrayList<>();

        for (int i = networkSize; i <= blockSize; i += networkSize) {
            networkSizeArray.add(i);
        }

        if (networkSize > blockSize) {
            networkSizeArray.add(blockSize);
        }

        return networkSizeArray;
    }

    List<Integer> getOptimizedNetworkSizeArray(int blockSize, int k) {
        List<Integer> networkSizeArray = new ArrayList<>();
        int incremental = blockSize / 2;
        int networkSize = incremental;
        int minBound = Math.max(blockSize * k / (1024 * 1024) * minBlockSize, minBlockSize);

        while (incremental >= minBound) {
            networkSizeArray.add(networkSize);
            networkSize += incremental / 2;
            incremental /= 2;
        }

        networkSizeArray.add(blockSize);

        return networkSizeArray;
    }

    List<Integer> getDegradeReadNetworkSize(int blockSize, int fixedSize) {
        List<Integer> networkSizeArray = getFixedNetworkSize(blockSize, fixedSize);

        if(networkSizeArray.size()!=1){
            int diff = blockSize - networkSizeArray.get(networkSizeArray.size()-1);
            List<Integer> decent=getNetworkSizeArray(fixedSize + diff);
            networkSizeArray.remove(networkSizeArray.size()-1);
            int base=networkSizeArray.get(networkSizeArray.size()-1);
            for (int i = 0; i < decent.size(); i++) {
                decent.set(i, decent.get(i)+base);
            }

            networkSizeArray.addAll(decent);
        }

        return networkSizeArray;
    }

    CrailFile writeMicroEC_CodingDescent(String filename, int size, int loop, int storageClass, int locationClass, int monECSubStripeNum, boolean skipDir) throws Exception {
        System.out.println("writeMicroEC_CodingDescent, filename " + filename + ", size " + size + ", loop " + loop + ", storageClass " + storageClass + ", locationClass " + locationClass + ", monECSubStripeNum " + monECSubStripeNum);

        // simulate object split
        int NumDataBlock = CrailConstants.ERASURECODING_K;
        int NumParityBlock = CrailConstants.ERASURECODING_M;
        // EC file can be processed in the same way with Replicas file
        CrailConstants.REPLICATION_FACTOR = NumDataBlock + NumParityBlock - 1;
        System.out.println("CrailConstants.REPLICATION_FACTOR has changed to " + CrailConstants.REPLICATION_FACTOR);

        Crailcoding codingLib = new Crailcoding(NumDataBlock, NumParityBlock);
        // pure ec split size
        int Microec_buffer_size = size / NumDataBlock;
        // monec encoding size
        int Microec_buffer_split_size = Microec_buffer_size / monECSubStripeNum;

//        List<Integer> networkSizeArray = getNetworkSizeArray(Microec_buffer_size);
        List<Integer> networkSizeArray = getOptimizedNetworkSizeArray(Microec_buffer_size, NumDataBlock);
        System.out.println("networkSizeArray " + networkSizeArray);
        int networkRound = networkSizeArray.size();

        ByteBuffer[] input = new ByteBuffer[NumDataBlock];
        ByteBuffer[] output = new ByteBuffer[NumParityBlock];
        ByteBuffer cursor = ByteBuffer.allocateDirect(monECSubStripeNum);
        String fullzero = getFullZeroString(monECSubStripeNum);
        ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<CrailBuffer> parityBufferQueue = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<CrailBuffer> transferQueue = new ConcurrentLinkedQueue<>();

        allocateDataBuffers(input, Microec_buffer_size, dataBufferQueue);
        allocateParityBuffers(output, Microec_buffer_size, parityBufferQueue);

        CodingCoreCache coreCache = new CodingCoreCache();
        int bindCoreIdx = 11;
        EncodingTask encodingTask = new EncodingTask(codingLib, monECSubStripeNum, NumDataBlock, NumParityBlock, Microec_buffer_size);
        ExecutorService es = Executors.newSingleThreadExecutor();

        // states of time log
        long codingSubmitStart, codingSubmitEnd, dataSubmitStart, dataSubmitEnd, paritySubmitStart, paritySubmitEnd, syncStart, syncEnd;
        long loopStart = 0, loopEnd = 0;
        long[] subParitySubmitStart = new long[networkRound];
        long[] subParitySubmitEnd = new long[networkRound];
        long[] subParityInLoopStart = new long[networkRound];
        long[] subParityInLoopEnd = new long[networkRound];
        long[] subPackingStart = new long[networkRound];
        long[] subPackingEnd = new long[networkRound];
        List<List<Long>> loopLatencyRecord = new ArrayList<>();
        List<List<Long>> loopTimestampRecord = new ArrayList<>();
        for (int i = 0; i < loop; i++) {
            loopLatencyRecord.add(new ArrayList<>());
            loopTimestampRecord.add(new ArrayList<>());
        }
        int ops = 0;
        everyLoopLatency = new long[loop];
        // Clear data and parity buffer before encoding (done in warmupMicroEC)
        // clear recorders' content
        Arrays.fill(subParitySubmitStart, 0);

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        long _bufsize = (long) Microec_buffer_size;

        CrailFile file = fs.createReplicasFile(filename, CrailNodeType.DATAFILE, CrailStorageClass.get(storageClass), CrailLocationClass.get(locationClass), !skipDir, RedundancyType.ERASURECODING_TYPE).get().asFile();
        CrailOutputStream directStream = ((ReplicasFile) file).getReplicasDirectOutputStream(_bufsize);
        getRedundantFileCapacity(file);

        // warm phrase
        // Goal: warm up data structures and function calls
        warmupMicroEC(directStream, loop, es, coreCache, encodingTask, input, output, dataBufferQueue, parityBufferQueue, transferQueue, cursor, networkSizeArray);

        while (ops < loop) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            clearBufferOfConcurrentLinkedQueue(parityBufferQueue);
            setByteBuffer(cursor, fullzero);
            ((CoreReplicasOutputStream) directStream).clearLatencyLogger();
            Arrays.fill(subParitySubmitStart, 0);
            ((CoreReplicasOutputStream) directStream).seek(0);

            // change data of data buffer
            randomDataBuffers(input, Microec_buffer_size);

//            if(ops == loop - 1000){
//                callShellByExec("/home/hadoop/testCrail/clear.sh");
//                callShellByExec("/home/hadoop/testCrail/usage.sh");
//            }

            loopStart = System.nanoTime();

            dataSubmitStart = System.nanoTime();
            // async send all data blocks
            ((CoreReplicasOutputStream) directStream).writeReplicasAsync(dataBufferQueue);
            dataSubmitEnd = System.nanoTime();

            codingSubmitStart = System.nanoTime();
            // set EncodingCodingTask (coding buffer + core idx)
//            bindCoreIdx = coreCache.getFreeCoreIdx();
//            bindCoreIdx = coreCache.randomGetFreeCoreIdx();
            bindCoreIdx = size > maxSmallObjSize ? coreCache.randomGetFreeCoreIdx() : coreCache.getFreeCoreIdx();
//            System.out.println("bindCoreIdx "+bindCoreIdx);
            encodingTask.setEncodingBuffer(input, output, cursor);
            encodingTask.setBindCoreidx(bindCoreIdx);
            // start encoding based on the size of the objects
            // block size of an object <= minBlockSize: sync coding
            // block size of an object > minBlockSize: async coding
            if (Microec_buffer_size <= minBlockSize || networkRound == 1) {
                codingLib.MicroecEncoding(input, output, cursor, monECSubStripeNum, NumDataBlock, NumParityBlock, Microec_buffer_size, bindCoreIdx);
            } else {
                es.submit(encodingTask);
            }
            codingSubmitEnd = System.nanoTime();

            paritySubmitStart = System.nanoTime();
            int networkIndex = 1;
            while (networkIndex <= networkRound) {
                int tmp = networkSizeArray.get(networkIndex - 1) / Microec_buffer_split_size - 1;

                if (subParitySubmitStart[networkIndex - 1] == 0) {
                    subParitySubmitStart[networkIndex - 1] = System.nanoTime();
                }

                if (cursor.get(tmp) == '1') {
                    subParityInLoopStart[networkIndex - 1] = System.nanoTime();
                    transferQueue.clear();
//                    subParityInLoopEnd[networkIndex-1]=System.nanoTime();
                    subPackingStart[networkIndex - 1] = System.nanoTime();
                    for (CrailBuffer buf : parityBufferQueue) {
                        buf.limit(networkSizeArray.get(networkIndex - 1));
                        if (networkIndex == 1) {
                            buf.position(0);
                        } else {
                            buf.position(networkSizeArray.get(networkIndex - 2));
                        }

                        CrailBuffer sbuf = buf.slice();
                        transferQueue.add(sbuf);
                    }
                    subPackingEnd[networkIndex - 1] = System.nanoTime();
//                    subParityInLoopEnd[networkIndex-1]=System.nanoTime();

//                    subParityInLoopStart[networkIndex-1]=System.nanoTime();
                    ((CoreReplicasOutputStream) directStream).writeParitySplitsAsync(transferQueue, NumDataBlock, NumParityBlock);
                    subParitySubmitEnd[networkIndex - 1] = System.nanoTime();
                    networkIndex++;
                }
            }
            paritySubmitEnd = System.nanoTime();

            syncStart = System.nanoTime();
            // sync all writes
            ((CoreReplicasOutputStream) directStream).syncResults();
            syncEnd = System.nanoTime();

            // release core idx
            coreCache.releaseCore();

            loopEnd = System.nanoTime();

//            if(ops == loop - 1){
//                callShellByExec("/home/hadoop/testCrail/usage.sh");
//            }

            everyLoopLatency[ops] = loopEnd - loopStart;
            loopLatencyRecord.get(ops).add(loopEnd - loopStart);
            loopLatencyRecord.get(ops).add((long) bindCoreIdx);
            loopLatencyRecord.get(ops).add(codingSubmitEnd - codingSubmitStart);
            loopLatencyRecord.get(ops).add(dataSubmitEnd - dataSubmitStart);
            loopLatencyRecord.get(ops).add(paritySubmitEnd - paritySubmitStart);
            for (int i = 0; i < networkRound; i++) {
//                loopLatencyRecord.get(ops).add(subParitySubmitEnd[i]-subParitySubmitStart[i]);
                loopLatencyRecord.get(ops).add(subParitySubmitEnd[i] - subParityInLoopStart[i]);
//                loopLatencyRecord.get(ops).add(subParityInLoopEnd[i]-subParityInLoopStart[i]);
                loopLatencyRecord.get(ops).add(subPackingEnd[i] - subPackingStart[i]);
            }
//            loopLatencyRecord.get(ops).addAll(((CoreReplicasOutputStream) directStream).getLatencyLogger());
            loopLatencyRecord.get(ops).add(syncEnd - syncStart);

            // save timestamp
            loopTimestampRecord.get(ops).add(loopStart);
            loopTimestampRecord.get(ops).add(dataSubmitStart);
            loopTimestampRecord.get(ops).add(dataSubmitEnd);
            loopTimestampRecord.get(ops).add(codingSubmitStart);
            loopTimestampRecord.get(ops).add(codingSubmitEnd);
            loopTimestampRecord.get(ops).add(paritySubmitStart);
            for (int i = 0; i < networkRound; i++) {
                loopTimestampRecord.get(ops).add(subParitySubmitStart[i]);
                loopTimestampRecord.get(ops).add(subParityInLoopStart[i]);
                loopTimestampRecord.get(ops).add(subPackingStart[i]);
                loopTimestampRecord.get(ops).add(subPackingEnd[i]);
                loopTimestampRecord.get(ops).add(subParitySubmitEnd[i]);
            }
            loopTimestampRecord.get(ops).add(paritySubmitEnd);
            loopTimestampRecord.get(ops).add(syncStart);
            loopTimestampRecord.get(ops).add(syncEnd);
            loopTimestampRecord.get(ops).add(loopEnd);

            ops += 1;
        }

        es.shutdown();
        directStream.close();

        System.out.println("ops " + ops);
        System.out.println("final loop (us) " + ((double) (loopEnd - loopStart)) / 1000.0);

        getAvgLoopLatency();

        System.out.println("Breakdown info");
        for (int i = 0; i < loopLatencyRecord.size(); i++) {
            System.out.println("Loop " + i + " " + loopLatencyRecord.get(i));
        }

        System.out.println("Timestamp info");
        for (int i = 0; i < loopTimestampRecord.size(); i++) {
            System.out.println("Timestamp " + i + " " + loopTimestampRecord.get(i));
        }

        getRedundantFileCapacity(file);

        fs.getStatistics().print("close");

        return file;
    }


    void writeMicroEC_CodingDescent_1loop(String filename, int size, int loop, int storageClass, int locationClass, int monECSubStripeNum, boolean skipDir) throws Exception {
        System.out.println("writeMicroEC_CodingDescent_1loop, filename " + filename + ", size " + size + ", loop " + loop + ", storageClass " + storageClass + ", locationClass " + locationClass + ", monECSubStripeNum " + monECSubStripeNum);

        // simulate object split
        int NumDataBlock = CrailConstants.ERASURECODING_K;
        int NumParityBlock = CrailConstants.ERASURECODING_M;

        Crailcoding codingLib = new Crailcoding(NumDataBlock, NumParityBlock);
        // pure ec split size
        int Microec_buffer_size = size / NumDataBlock;
        // monec encoding size
        int Microec_buffer_split_size = Microec_buffer_size / monECSubStripeNum;

        List<Integer> networkSizeArray = getNetworkSizeArray(Microec_buffer_size);
        System.out.println("networkSizeArray " + networkSizeArray);
        int networkRound = networkSizeArray.size();

        ByteBuffer[] input = new ByteBuffer[NumDataBlock];
        ByteBuffer[] output = new ByteBuffer[NumParityBlock];
        ByteBuffer cursor = ByteBuffer.allocateDirect(monECSubStripeNum);
        cursor.clear();
        String fullzero = getFullZeroString(monECSubStripeNum);
        ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<CrailBuffer> parityBufferQueue = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<CrailBuffer> transferQueue = new ConcurrentLinkedQueue<>();

        allocateDataBuffers(input, Microec_buffer_size, dataBufferQueue);
        allocateParityBuffers(output, Microec_buffer_size, parityBufferQueue);

        CodingCoreCache coreCache = new CodingCoreCache();
        EncodingTask encodingTask = new EncodingTask(codingLib, monECSubStripeNum, NumDataBlock, NumParityBlock, Microec_buffer_size);
        ExecutorService es = Executors.newSingleThreadExecutor();

        // states of time log
        long codingSubmitStart, codingSubmitEnd, dataSubmitStart, dataSubmitEnd, paritySubmitStart, paritySubmitEnd, syncStart, syncEnd;
        long[] subParitySubmitStart = new long[networkRound];
        long[] subParitySubmitEnd = new long[networkRound];
        long[] subParityInLoopStart = new long[networkRound];
        long[] subParityInLoopEnd = new long[networkRound];

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        long _bufsize = (long) Microec_buffer_size;

        // EC file can be processed in the same way with Replicas file
        CrailConstants.REPLICATION_FACTOR = NumDataBlock + NumParityBlock - 1;
        System.out.println("CrailConstants.REPLICATION_FACTOR has changed to " + CrailConstants.REPLICATION_FACTOR);

        CrailFile file = fs.createReplicasFile(filename, CrailNodeType.DATAFILE, CrailStorageClass.get(storageClass), CrailLocationClass.get(locationClass), !skipDir, RedundancyType.ERASURECODING_TYPE).get().asFile();
        CrailOutputStream directStream = ((ReplicasFile) file).getReplicasDirectOutputStream(_bufsize);
        getRedundantFileCapacity(file);

        // Clear data and parity buffer before encoding
        clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
        clearBufferOfConcurrentLinkedQueue(parityBufferQueue);
        setByteBuffer(cursor, fullzero);

        // clear recorders' content
        Arrays.fill(subParitySubmitStart, 0);
        ((CoreReplicasOutputStream) directStream).clearLatencyLogger();
        long loopStart = 0, loopEnd = 0;

        // warm phrase
        // Goal: warm up data structures and function calls
//        warmupConcurrentLinkedQueue(loop, dataBufferQueue, parityBufferQueue, transferQueue);
//        warmupByteBuffer(loop, cursor);
//        warmupCreateRedundantFile(directStream, loop, Microec_buffer_size);
//        warmupCreateRedundantFile(directStream, loop, input, output, dataBufferQueue, parityBufferQueue);
//        warmupCoding(loop, es, coreCache, EncodingCodingTask);
//        warmupMicroEC(filename, loop, es, coreCache, EncodingCodingTask);
//        warmupMicroEC(directStream, loop, es, coreCache, EncodingCodingTask, output, dataBufferQueue, parityBufferQueue, transferQueue, cursor, networkSizeArray);
        warmupMicroEC(directStream, loop, es, coreCache, encodingTask, input, output, dataBufferQueue, parityBufferQueue, transferQueue, cursor, networkSizeArray);

        // change data of data buffer
        randomDataBuffers(input, Microec_buffer_size);
        System.out.println("Start timing...");
        loopStart = System.nanoTime();

        codingSubmitStart = System.nanoTime();
        // set EncodingCodingTask (coding buffer + core idx)
        int bindCoreIdx = coreCache.getFreeCoreIdx();
        encodingTask.setEncodingBuffer(input, output, cursor);
        encodingTask.setBindCoreidx(bindCoreIdx);
        // start encoding based on the size of the objects
        // block size of an object <= minBlockSize: sync coding
        // block size of an object > minBlockSize: async coding
        if (Microec_buffer_size <= minBlockSize) {
            codingLib.MicroecEncoding(input, output, cursor, monECSubStripeNum, NumDataBlock, NumParityBlock, Microec_buffer_size, bindCoreIdx);
        } else {
            es.submit(encodingTask);
        }
        codingSubmitEnd = System.nanoTime();

        dataSubmitStart = System.nanoTime();
        // async send all data blocks
        ((CoreReplicasOutputStream) directStream).writeReplicasAsync(dataBufferQueue);
        dataSubmitEnd = System.nanoTime();

        paritySubmitStart = System.nanoTime();
        int networkIndex = 1;
        while (networkIndex <= networkRound) {
            int tmp = networkSizeArray.get(networkIndex - 1) / Microec_buffer_split_size - 1;

            if (subParitySubmitStart[networkIndex - 1] == 0) {
                subParitySubmitStart[networkIndex - 1] = System.nanoTime();
            }

            if (cursor.get(tmp) == '1') {
                subParityInLoopStart[networkIndex - 1] = System.nanoTime();

                transferQueue.clear();
//                    subParityInLoopEnd[networkIndex-1]=System.nanoTime();
                for (CrailBuffer buf : parityBufferQueue) {
//                        System.out.println("buf info before:" + buf.position() + " " + buf.limit() + " " + buf.capacity());
                    buf.limit(networkSizeArray.get(networkIndex - 1));
                    if (networkIndex == 1) {
                        buf.position(0);
                    } else {
                        buf.position(networkSizeArray.get(networkIndex - 2));
                    }

//                      System.out.println("buf info after:" + buf.position() + " " + buf.limit() + " " + buf.capacity());

                    CrailBuffer sbuf = buf.slice();
                    transferQueue.add(sbuf);
                }
//                    subParityInLoopEnd[networkIndex-1]=System.nanoTime();

//                    subParityInLoopStart[networkIndex-1]=System.nanoTime();
                ((CoreReplicasOutputStream) directStream).writeParitySplitsAsync(transferQueue, NumDataBlock, NumParityBlock);
                subParitySubmitEnd[networkIndex - 1] = System.nanoTime();
                networkIndex++;
            }
        }
        paritySubmitEnd = System.nanoTime();

        syncStart = System.nanoTime();
        // sync all writes
        ((CoreReplicasOutputStream) directStream).syncResults();
        syncEnd = System.nanoTime();

        // release core idx
        coreCache.releaseCore();

        loopEnd = System.nanoTime();

        es.shutdown();
        directStream.close();

        System.out.println("Latency (us) " + (loopEnd - loopStart) / 1000.0);
        System.out.println("Coding submit latency (us) " + (codingSubmitEnd - codingSubmitStart) / 1000.0);
        System.out.println("Data submit latency (us) " + (dataSubmitEnd - dataSubmitStart) / 1000.0);
        System.out.println("Parity submit latency (us) " + (paritySubmitEnd - paritySubmitStart) / 1000.0);
        for (int i = 0; i < networkRound; i++) {
            System.out.println("Network round " + i + " subParity submit latency (us) " + (subParitySubmitEnd[i] - subParityInLoopStart[i]) / 1000.0);
        }
        System.out.println("Sync latency (us) " + (syncEnd - syncStart) / 1000.0);

        getRedundantFileCapacity(file);

        fs.getStatistics().print("close");
    }

    void printFileType(CrailFile file) {
        if (file instanceof ReplicasFile) {
            System.out.println(file.getPath() + " is a ReplicasFile!");
        } else if (file instanceof CoreFile) {
            System.out.println(file.getPath() + " is a CoreFile!");
        } else {
            System.out.println(file.getPath() + " is unknown!");
        }
    }

    void warmupReadReplicas(CrailInputStream directStream, int operations, CrailBuffer buf) throws Exception {
        for (int i = 0; i < operations; i++) {
            buf.clear();
            directStream.seek(0);
            double ret = (double) directStream.read(buf).get().getLen();
        }
    }

    void readReplicas(String filename, int loop) throws Exception {
        System.out.println("readReplicas, filename " + filename + ", loop " + loop);

        everyLoopLatency = new long[loop];

        CrailFile file = fs.lookup(filename).get().asFile();
        printFileType(file);

        long size = file.getCapacity();
        CrailInputStream directStream = file.getDirectInputStream(size);
        CrailBuffer buf = OffHeapBuffer.wrap(ByteBuffer.allocateDirect((int) size));

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        int ops = 0;
        long loopStart = 0, loopEnd = 0;

        warmupReadReplicas(directStream, loop, buf);
        while (ops < loop) {
            buf.clear();
            directStream.seek(0);
            loopStart = System.nanoTime();
            double ret = (double) directStream.read(buf).get().getLen();
            loopEnd = System.nanoTime();

            everyLoopLatency[ops] = loopEnd - loopStart;
            ops = ops + 1;
        }

        directStream.close();

        System.out.println("ops " + ops);
        System.out.println("final loop (us) " + ((double) (loopEnd - loopStart)) / 1000.0);
        getAvgLoopLatency();

        fs.getStatistics().print("close");
    }

    void warmupReadErasureCoding(CrailInputStream directStream, int operations, ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue) throws Exception {
        for (int i = 0; i < operations; i++) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            ((CoreErasureCodingInputStream) directStream).resetStreams();

            ((CoreErasureCodingInputStream) directStream).readSplitsAsync(dataBufferQueue);
            double ret = ((CoreErasureCodingInputStream) directStream).syncResults();
        }
    }

    void readErasureCoding(String filename, int loop) throws Exception {
        System.out.println("readErasureCoding, filename " + filename + ", loop " + loop);

//        if (!CrailConstants.REDUNDANCY_TYPE.equals("erasurecoding")) {
//            throw new Exception("CrailConstants.REDUNDANCY_TYPE is not equal to erasure coding!");
//        }
        CrailConstants.REPLICATION_FACTOR = CrailConstants.ERASURECODING_K + CrailConstants.ERASURECODING_M - 1;

        everyLoopLatency = new long[loop];

        CrailFile file = fs.lookup(filename).get().asFile();
        printFileType(file);

        // allocate buffer for storing read data
        // single split size
        long size = file.getCapacity();
        ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        allocateDataBuffers(dataBufferQueue, (int) size);

        CrailInputStream directStream = ((ReplicasFile) file).getErasureCodingDirectInputStream(size);

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        int ops = 0;
        long loopStart = 0, loopEnd = 0;

        warmupReadErasureCoding(directStream, loop, dataBufferQueue);
        while (ops < loop) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            ((CoreErasureCodingInputStream) directStream).resetStreams();

            loopStart = System.nanoTime();
            ((CoreErasureCodingInputStream) directStream).readSplitsAsync(dataBufferQueue);
            double ret = ((CoreErasureCodingInputStream) directStream).syncResults();
            loopEnd = System.nanoTime();

            everyLoopLatency[ops] = loopEnd - loopStart;
            ops = ops + 1;
        }

        ((CoreErasureCodingInputStream) directStream).closeSlavesInputStream();

        System.out.println("ops " + ops);
        System.out.println("final loop (us) " + ((double) (loopEnd - loopStart)) / 1000.0);
        getAvgLoopLatency();

        fs.getStatistics().print("close");
    }

    void degradeReadReplicas(String filename, int loop) throws Exception {
        System.out.println("degradeReadReplicas, filename " + filename + ", loop " + loop);

//        if (!CrailConstants.REDUNDANCY_TYPE.equals("replicas")) {
//            throw new Exception("CrailConstants.REDUNDANCY_TYPE is not equal to replicas!");
//        }
        everyLoopLatency = new long[loop];

        CrailFile file = fs.lookup(filename).get().asFile();
        printFileType(file);

        // allocate buffer for storing read data
        // single split size
        long size = file.getCapacity();
        CrailBuffer dataBuf = OffHeapBuffer.wrap(ByteBuffer.allocateDirect((int) size));
        CrailInputStream directStream = ((ReplicasFile) file).getDegradeReplicasDirectInputStream(size, degradeIndices);

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        int ops = 0;
        long loopStart = 0, loopEnd = 0;

        warmupReadReplicas(directStream, loop, dataBuf);
        while (ops < loop) {
            dataBuf.clear();
            directStream.seek(0);

            loopStart = System.nanoTime();
            double ret = (double) directStream.read(dataBuf).get().getLen();
            loopEnd = System.nanoTime();

            everyLoopLatency[ops] = loopEnd - loopStart;
            ops = ops + 1;
        }

        directStream.close();

        System.out.println("ops " + ops);
        System.out.println("final loop (us) " + ((double) (loopEnd - loopStart)) / 1000.0);
        getAvgLoopLatency();

        fs.getStatistics().print("close");
    }

    void warmupRecoveryReplicas(CrailInputStream directStream, int operations, CrailBuffer dataBuf, List<CrailOutputStream> recoverOutputStreams, LinkedBlockingQueue<Future<CrailResult>> futureQueue) throws Exception {
        int degradeNum = recoverOutputStreams.size();
        int ops = 0;

        while (ops++ < operations) {
            dataBuf.clear();
            directStream.seek(0);
            for (int i = 0; i < degradeNum; i++) {
                CrailOutputStream stream = recoverOutputStreams.get(i);
                ((CoreOutputStream) stream).seek(0);
            }

            double ret = (double) directStream.read(dataBuf).get().getLen();
            // the write of each recovery splits is async
            for (int i = 0; i < degradeNum; i++) {
                dataBuf.clear();
                CrailBuffer tmp = dataBuf.slice();
                futureQueue.add(recoverOutputStreams.get(i).write(tmp));
            }
            while (!futureQueue.isEmpty()) {
                Future<CrailResult> future = futureQueue.poll();
                future.get();
            }
        }
    }

    void recoveryReplicas(String filename, int loop) throws Exception {
        System.out.println("recoveryReplicas, filename " + filename + ", loop " + loop);

//        if (!CrailConstants.REDUNDANCY_TYPE.equals("replicas")) {
//            throw new Exception("CrailConstants.REDUNDANCY_TYPE is not equal to replicas!");
//        }
        everyLoopLatency = new long[loop];

        CrailFile file = fs.lookup(filename).get().asFile();
        printFileType(file);

        // allocate buffer for storing read data
        // single split size
        long size = file.getCapacity();
        CrailBuffer dataBuf = OffHeapBuffer.wrap(ByteBuffer.allocateDirect((int) size));
        CrailInputStream directStream = ((ReplicasFile) file).getDegradeReplicasDirectInputStream(size, degradeIndices);

        // create a recovery file
        List<CrailFile> recoverFiles = new ArrayList<>();
        List<CrailOutputStream> recoverOutputStreams = new ArrayList<>();
        LinkedBlockingQueue<Future<CrailResult>> futureQueue = new LinkedBlockingQueue<>();
        for (int i = 0; i < degradeNum; i++) {
            CrailFile rf = fs.create(filename + ".replicasrec" + i, file.getType(), CrailStorageClass.get(0), CrailLocationClass.get(0), !false).get().asFile();
            CrailOutputStream recoverDirectStream = rf.getDirectOutputStream(size);
            recoverFiles.add(rf);
            recoverOutputStreams.add(recoverDirectStream);
        }

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        int ops = 0;
        long loopStart = 0, loopEnd = 0;

        warmupRecoveryReplicas(directStream, loop, dataBuf, recoverOutputStreams, futureQueue);
        while (ops < loop) {
            dataBuf.clear();
            directStream.seek(0);
            for (int i = 0; i < degradeNum; i++) {
                CrailOutputStream stream = recoverOutputStreams.get(i);
                ((CoreOutputStream) stream).seek(0);
            }

            loopStart = System.nanoTime();
            double ret = (double) directStream.read(dataBuf).get().getLen();
            // the write of each recovery splits is async
            for (int i = 0; i < degradeNum; i++) {
                dataBuf.clear();
                CrailBuffer tmp = dataBuf.slice();
                futureQueue.add(recoverOutputStreams.get(i).write(tmp));
            }
            while (!futureQueue.isEmpty()) {
                Future<CrailResult> future = futureQueue.poll();
                future.get();
            }
            loopEnd = System.nanoTime();

            everyLoopLatency[ops] = loopEnd - loopStart;
            ops = ops + 1;
        }

        directStream.close();
        for (int i = 0; i < degradeNum; i++) {
            recoverOutputStreams.get(i).close();
        }

        System.out.println("ops " + ops);
        System.out.println("final loop (us) " + ((double) (loopEnd - loopStart)) / 1000.0);
        getAvgLoopLatency();
        getRedundantFileCapacity(recoverFiles);

        fs.getStatistics().print("close");
    }

    int[] getDegradeSrcIndices(int[] degradeIndices) {
        int degradeNum = degradeIndices.length;
        int NumDataBlock = CrailConstants.ERASURECODING_K;
        int NumParityBlock = CrailConstants.ERASURECODING_M;

        // get srcIndices
        int[] srcIndices = new int[NumDataBlock];
        int k = 0;
        for (int i = 0; i < (NumDataBlock + NumParityBlock); i++) {
            boolean flag = true;
            for (int j = 0; j < degradeNum; j++) {
                if (degradeIndices[j] == i) {
                    flag = false;
                    break;
                }
            }
            if (flag) {
                srcIndices[k] = i;
//                System.out.println("srcIndices " + k + ": " + srcIndices[k]);
                k++;
                if (k == NumDataBlock) {
                    break;
                }
            }
        }

        return srcIndices;
    }

    void warmupDegradeReadErasureCoding(CrailInputStream directStream, int operations, int NumDataBlock, int NumParityBlock, int size, ByteBuffer[] src, ByteBuffer[] eraseData, int[] srcIndices, ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue, ConcurrentLinkedQueue<CrailBuffer> eraseBufferQueue, Crailcoding codingLib, CodingCoreCache coreCache) throws Exception {
        int ops = 0;
        int bindCoreIdx = 11;
        while (ops++ < operations) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            clearBufferOfConcurrentLinkedQueue(eraseBufferQueue);
            ((CoreErasureCodingInputStream) directStream).resetStreams();

            ((CoreErasureCodingInputStream) directStream).readSplitsAsync(dataBufferQueue);
            double ret = ((CoreErasureCodingInputStream) directStream).syncResults();
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            bindCoreIdx = coreCache.randomGetFreeCoreIdx();
            codingLib.NativeDecoding(src, eraseData, srcIndices, degradeIndices, NumDataBlock, NumParityBlock, size, degradeNum, bindCoreIdx);
            coreCache.releaseCore();
        }
    }

    void degradeReadErasureCoding(String filename, int loop) throws Exception {
        System.out.println("degradeReadErasureCoding, filename " + filename + ", loop " + loop);

//        if (!CrailConstants.REDUNDANCY_TYPE.equals("erasurecoding")) {
//            throw new Exception("CrailConstants.REDUNDANCY_TYPE is not equal to erasure coding!");
//        }
        CrailConstants.REPLICATION_FACTOR = CrailConstants.ERASURECODING_K + CrailConstants.ERASURECODING_M - 1;
        int NumDataBlock = CrailConstants.ERASURECODING_K;
        int NumParityBlock = CrailConstants.ERASURECODING_M;
        Crailcoding codingLib = new Crailcoding(NumDataBlock, NumParityBlock);
        CodingCoreCache coreCache = new CodingCoreCache();
        int bindCoreIdx = 11;

        everyLoopLatency = new long[loop];

        // test
        int[] srcIndices = getDegradeSrcIndices(degradeIndices);

        CrailFile file = fs.lookup(filename).get().asFile();
        printFileType(file);

        // allocate buffer for storing read data
        // single split size
        long size = file.getCapacity();
        ByteBuffer[] src = new ByteBuffer[NumDataBlock];
        ByteBuffer[] eraseData = new ByteBuffer[degradeNum];
        ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        ConcurrentLinkedQueue<CrailBuffer> eraseBufferQueue = new ConcurrentLinkedQueue<>();
        allocateDataBuffers(src, (int) size, dataBufferQueue);
        allocateParityBuffers(eraseData, (int) size, eraseBufferQueue);
        CrailInputStream directStream = ((ReplicasFile) file).getDegradeErasureCodingDirectInputStream(size, degradeIndices);

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        int ops = 0;
        long loopStart = 0, loopEnd = 0;

        warmupDegradeReadErasureCoding(directStream, loop, NumDataBlock, NumParityBlock, (int) size, src, eraseData, srcIndices, dataBufferQueue, eraseBufferQueue, codingLib, coreCache);
        while (ops < loop) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            clearBufferOfConcurrentLinkedQueue(eraseBufferQueue);
            ((CoreErasureCodingInputStream) directStream).resetStreams();

            loopStart = System.nanoTime();
            ((CoreErasureCodingInputStream) directStream).readSplitsAsync(dataBufferQueue);
            double ret = ((CoreErasureCodingInputStream) directStream).syncResults();
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
//            bindCoreIdx = coreCache.randomGetFreeCoreIdx();
//            bindCoreIdx = coreCache.getFreeCoreIdx();
            bindCoreIdx = size > maxSmallObjSize ? coreCache.randomGetFreeCoreIdx() : coreCache.getFreeCoreIdx();
            codingLib.NativeDecoding(src, eraseData, srcIndices, degradeIndices, NumDataBlock, NumParityBlock, (int) size, degradeNum, bindCoreIdx);
            coreCache.releaseCore();
            loopEnd = System.nanoTime();

            everyLoopLatency[ops] = loopEnd - loopStart;
            ops = ops + 1;
        }

        ((CoreErasureCodingInputStream) directStream).closeSlavesInputStream();

        System.out.println("ops " + ops);
        System.out.println("final loop (us) " + ((double) (loopEnd - loopStart)) / 1000.0);
        getAvgLoopLatency();

        fs.getStatistics().print("close");
    }

    void warmupRecoveryErasureCoding(CrailInputStream directStream, int operations, int NumDataBlock, int NumParityBlock, int size, ByteBuffer[] src, ByteBuffer[] eraseData, int[] srcIndices, ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue, ConcurrentLinkedQueue<CrailBuffer> eraseBufferQueue, List<CrailOutputStream> recoverOutputStreams, LinkedBlockingQueue<Future<CrailResult>> futureQueue, Crailcoding codingLib, CodingCoreCache coreCache) throws Exception {
        int ops = 0;
        int bindCoreIdx = 11;
        while (ops++ < operations) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            clearBufferOfConcurrentLinkedQueue(eraseBufferQueue);
            ((CoreErasureCodingInputStream) directStream).resetStreams();
            for (int i = 0; i < degradeNum; i++) {
                ((CoreOutputStream) recoverOutputStreams.get(i)).seek(0);
            }

            ((CoreErasureCodingInputStream) directStream).readSplitsAsync(dataBufferQueue);
            double ret = ((CoreErasureCodingInputStream) directStream).syncResults();

            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            bindCoreIdx = coreCache.randomGetFreeCoreIdx();
            codingLib.NativeDecoding(src, eraseData, srcIndices, degradeIndices, NumDataBlock, NumParityBlock, (int) size, degradeNum, bindCoreIdx);
            coreCache.releaseCore();

            // the write of each recovery splits is async
            int i = 0;
            for (CrailBuffer buf : eraseBufferQueue) {
                futureQueue.add(recoverOutputStreams.get(i++).write(buf));
            }
            while (!futureQueue.isEmpty()) {
                Future<CrailResult> future = futureQueue.poll();
                future.get();
            }
        }
    }

    // recovery a file in ec-cache
    void recoveryErasureCoding(String filename, int loop) throws Exception {
        System.out.println("recoveryErasureCoding, filename " + filename + ", loop " + loop);

//        if (!CrailConstants.REDUNDANCY_TYPE.equals("erasurecoding")) {
//            throw new Exception("CrailConstants.REDUNDANCY_TYPE is not equal to erasure coding!");
//        }
        CrailConstants.REPLICATION_FACTOR = CrailConstants.ERASURECODING_K + CrailConstants.ERASURECODING_M - 1;
        int NumDataBlock = CrailConstants.ERASURECODING_K;
        int NumParityBlock = CrailConstants.ERASURECODING_M;
        Crailcoding codingLib = new Crailcoding(NumDataBlock, NumParityBlock);
        CodingCoreCache coreCache = new CodingCoreCache();
        int bindCoreIdx = 11;

        everyLoopLatency = new long[loop];

        // test
        int[] srcIndices = getDegradeSrcIndices(degradeIndices);

        CrailFile file = fs.lookup(filename).get().asFile();
        printFileType(file);

        // allocate buffer for storing read data
        // single split size
        long size = file.getCapacity();
        ByteBuffer[] src = new ByteBuffer[NumDataBlock];
        ByteBuffer[] eraseData = new ByteBuffer[degradeNum];
        ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        ConcurrentLinkedQueue<CrailBuffer> eraseBufferQueue = new ConcurrentLinkedQueue<>();
        allocateDataBuffers(src, (int) size, dataBufferQueue);
        allocateParityBuffers(eraseData, (int) size, eraseBufferQueue);
        CrailInputStream directStream = ((ReplicasFile) file).getDegradeErasureCodingDirectInputStream(size, degradeIndices);

        // create a recovery file
        List<CrailFile> recoverFiles = new ArrayList<>();
        List<CrailOutputStream> recoverOutputStreams = new ArrayList<>();
        LinkedBlockingQueue<Future<CrailResult>> futureQueue = new LinkedBlockingQueue<>();
        for (int i = 0; i < degradeNum; i++) {
            CrailFile rf = fs.create(filename + ".nativerec" + i, file.getType(), CrailStorageClass.get(0), CrailLocationClass.get(0), !false).get().asFile();
            CrailOutputStream recoverDirectStream = rf.getDirectOutputStream(size);
            recoverFiles.add(rf);
            recoverOutputStreams.add(recoverDirectStream);
        }

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        int ops = 0;
        long loopStart = 0, loopEnd = 0;

        warmupRecoveryErasureCoding(directStream, loop, NumDataBlock, NumParityBlock, (int) size, src, eraseData, srcIndices, dataBufferQueue, eraseBufferQueue, recoverOutputStreams, futureQueue, codingLib, coreCache);
        while (ops < loop) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            clearBufferOfConcurrentLinkedQueue(eraseBufferQueue);
            ((CoreErasureCodingInputStream) directStream).resetStreams();
            for (int i = 0; i < degradeNum; i++) {
                ((CoreOutputStream) recoverOutputStreams.get(i)).seek(0);
            }

            loopStart = System.nanoTime();
            ((CoreErasureCodingInputStream) directStream).readSplitsAsync(dataBufferQueue);
            double ret = ((CoreErasureCodingInputStream) directStream).syncResults();

            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
//            bindCoreIdx = coreCache.randomGetFreeCoreIdx();
//            bindCoreIdx = coreCache.getFreeCoreIdx();
            bindCoreIdx = size > maxSmallObjSize ? coreCache.randomGetFreeCoreIdx() : coreCache.getFreeCoreIdx();
            codingLib.NativeDecoding(src, eraseData, srcIndices, degradeIndices, NumDataBlock, NumParityBlock, (int) size, degradeNum, bindCoreIdx);
            coreCache.releaseCore();

            // the write of each recovery splits is async
            int i = 0;
            for (CrailBuffer buf : eraseBufferQueue) {
                futureQueue.add(recoverOutputStreams.get(i++).write(buf));
            }
            while (!futureQueue.isEmpty()) {
                Future<CrailResult> future = futureQueue.poll();
                future.get();
            }
            loopEnd = System.nanoTime();

            everyLoopLatency[ops] = loopEnd - loopStart;
            ops = ops + 1;
        }

        ((CoreErasureCodingInputStream) directStream).closeSlavesInputStream();
        for (int i = 0; i < degradeNum; i++) {
            recoverOutputStreams.get(i).close();
        }

        System.out.println("ops " + ops);
        System.out.println("final loop (us) " + ((double) (loopEnd - loopStart)) / 1000.0);
        getAvgLoopLatency();
        getRedundantFileCapacity(recoverFiles);

        fs.getStatistics().print("close");
    }

    void zeroByteBuffer(ByteBuffer buf) {
        for (int i = 0; i < buf.capacity(); i++) {
            buf.put(i, (byte) 0);
        }
        buf.clear();
    }

    void setCounter(ByteBuffer buf, int num) {
        buf.put(0, (byte) (num / 127));
        buf.put(1, (byte) (num % 127));
    }

    int getCounter(ByteBuffer buf) {
        return buf.get(0) * 127 + buf.get(1);
    }

    void warmupDegradeReadMicroEC(CrailInputStream directStream, int operations, DecodingTask decodingTask, ExecutorService es, ByteBuffer[] src, ByteBuffer[] eraseData, ByteBuffer cursor, int[] srcIndices, ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue, ConcurrentLinkedQueue<CrailBuffer> eraseBufferQueue, ConcurrentLinkedQueue<CrailBuffer> transferBufferQueue, ByteBuffer bufHasTransfered, ByteBuffer bufHasCodedBuf, Crailcoding codingLib, CodingCoreCache coreCache, List<Integer> networkSizeArray) throws Exception {
        int networkRound = networkSizeArray.size();
        int NumDataBlock = decodingTask.NumDataBlock;
        int NumParityBlock = decodingTask.NumParityBlock;
        int size = decodingTask.Microec_buffer_size;
        int monECSubStripeNum = decodingTask.NumSubStripe;
        int Monec_buffer_split_size = size / monECSubStripeNum;
        String fullzero = getFullZeroString(monECSubStripeNum);
        int cursor_network = 0;
        int bindCoreIdx = 11;
        int ops = 0;
        while (ops++ < operations) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            clearBufferOfConcurrentLinkedQueue(eraseBufferQueue);
            setByteBuffer(cursor, fullzero);
            cursor_network = 0;
            zeroByteBuffer(bufHasTransfered);
            zeroByteBuffer(bufHasCodedBuf);
            ((CoreErasureCodingInputStream) directStream).resetStreams();

            // start decoding process
            bindCoreIdx = coreCache.randomGetFreeCoreIdx();
            decodingTask.setDecodingBuffer(src, eraseData, cursor);
            decodingTask.setCodingCounter(bufHasTransfered, bufHasCodedBuf);
            decodingTask.setBindCoreidx(bindCoreIdx);
            if (networkRound > 1) {
                es.submit(decodingTask);
            }

            for (int i = 0; i < networkRound; i++) {
                transferBufferQueue.clear();
                for (CrailBuffer buf : dataBufferQueue) {
                    buf.limit(networkSizeArray.get(i));
                    if (i == 0) {
                        buf.position(0);
                    } else {
                        buf.position(networkSizeArray.get(i - 1));
                    }
                    transferBufferQueue.add(buf);
                }
                ((CoreErasureCodingInputStream) directStream).readSplitsAsync(transferBufferQueue);
                double ret = ((CoreErasureCodingInputStream) directStream).syncResults();

                if (networkRound == 1) {
                    codingLib.NativeDecoding(src, eraseData, srcIndices, degradeIndices, NumDataBlock, NumParityBlock, size, degradeNum, bindCoreIdx);
                } else {
                    cursor_network = networkSizeArray.get(i) / Monec_buffer_split_size;
                    setCounter(bufHasTransfered, cursor_network);
                }
            }

            // check decoding is whether finished
            while (networkRound != 1 && !(cursor.get(monECSubStripeNum - 1) == '1')) {
                // blocking until decoding finishing
            }
            coreCache.releaseCore();
        }
    }

    // microec pipeline degrade read
    void degradedReadMicroEC(String filename, int monECSubStripeNum, int transferSize, int loop) throws Exception {
        System.out.println("degradedReadMicroEC, filename " + filename + " monECSubStripeNum " + monECSubStripeNum + " transferSize " + transferSize + " loop " + loop + " degradeNum " + degradeNum);

//        if (!CrailConstants.REDUNDANCY_TYPE.equals("erasurecoding")) {
//            throw new Exception("CrailConstants.REDUNDANCY_TYPE is not equal to erasure coding!");
//        }
        CrailConstants.REPLICATION_FACTOR = CrailConstants.ERASURECODING_K + CrailConstants.ERASURECODING_M - 1;
        int NumDataBlock = CrailConstants.ERASURECODING_K;
        int NumParityBlock = CrailConstants.ERASURECODING_M;
        Crailcoding codingLib = new Crailcoding(NumDataBlock, NumParityBlock);
        CodingCoreCache coreCache = new CodingCoreCache();
        int bindCoreIdx = 11;

        // test, one straggler degrade read
        int[] srcIndices = getDegradeSrcIndices(degradeIndices);

        CrailFile file = fs.lookup(filename).get().asFile();
        printFileType(file);

        // allocate buffer for storing read data
        // single split size
        long size = file.getCapacity();
        // pure ec split size
        int Microec_buffer_size = (int) size;
        // monec encoding size
        int Monec_buffer_split_size = Microec_buffer_size / monECSubStripeNum;
        // network transfer size list
        List<Integer> networkSizeArray = getDegradeReadNetworkSize(Microec_buffer_size, transferSize);
        int networkRound = networkSizeArray.size();
        System.out.println("networkSizeArray " + networkSizeArray);

        // store read src data
        ByteBuffer[] src = new ByteBuffer[NumDataBlock];
        ByteBuffer[] eraseData = new ByteBuffer[degradeNum];
        ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        ConcurrentLinkedQueue<CrailBuffer> eraseBufferQueue = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<CrailBuffer> transferBufferQueue = new ConcurrentLinkedQueue<>();
        allocateDataBuffers(src, Microec_buffer_size, dataBufferQueue);
        allocateParityBuffers(eraseData, Microec_buffer_size, eraseBufferQueue);
        ByteBuffer cursor = ByteBuffer.allocateDirect(monECSubStripeNum);
        ByteBuffer bufHasTransfered = ByteBuffer.allocateDirect(2);
        ByteBuffer bufHasCodedBuf = ByteBuffer.allocateDirect(2);
        String fullzero = getFullZeroString(monECSubStripeNum);
        setByteBuffer(cursor, fullzero);
        // transfer buffer
        int cursor_network = 0, hasCoded = 0;
        CrailInputStream directStream = ((ReplicasFile) file).getDegradeErasureCodingDirectInputStream(Microec_buffer_size, degradeIndices);

        DecodingTask decodingTask = new DecodingTask(codingLib, monECSubStripeNum, NumDataBlock, NumParityBlock, Microec_buffer_size, srcIndices, degradeIndices, degradeNum);
        ExecutorService es = Executors.newSingleThreadExecutor();

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        int ops = 0;
        everyLoopLatency = new long[loop];
        long loopStart = 0, loopEnd = 0;
        long codingSubmitStart, codingSubmitEnd, transferStart, transferEnd, syncStart, syncEnd;
        long[] subTransferStart = new long[networkRound];
        long[] subTransferEnd = new long[networkRound];
        long[] subUpdateStart = new long[networkRound];
        long[] subUpdateEnd = new long[networkRound];
        int[] subHasCoded = new int[networkRound];
        List<List<Long>> loopLatencyRecord = new ArrayList<>();
        for (int i = 0; i < loop; i++) {
            loopLatencyRecord.add(new ArrayList<>());
        }

        warmupDegradeReadMicroEC(directStream, loop, decodingTask, es, src, eraseData, cursor, srcIndices, dataBufferQueue, eraseBufferQueue, transferBufferQueue, bufHasTransfered, bufHasCodedBuf, codingLib, coreCache, networkSizeArray);
        while (ops < loop) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            clearBufferOfConcurrentLinkedQueue(eraseBufferQueue);
            setByteBuffer(cursor, fullzero);
            cursor_network = 0;
            hasCoded = 0;
            zeroByteBuffer(bufHasTransfered);
            zeroByteBuffer(bufHasCodedBuf);
            ((CoreErasureCodingInputStream) directStream).resetStreams();

//            System.out.println("ops "+ops);
            loopStart = System.nanoTime();
            codingSubmitStart = System.nanoTime();
            // start decoding process
//            bindCoreIdx = coreCache.randomGetFreeCoreIdx();
//            bindCoreIdx = coreCache.getFreeCoreIdx();
            bindCoreIdx = size > maxSmallObjSize ? coreCache.randomGetFreeCoreIdx() : coreCache.getFreeCoreIdx();
            decodingTask.setDecodingBuffer(src, eraseData, cursor);
            decodingTask.setCodingCounter(bufHasTransfered, bufHasCodedBuf);
            decodingTask.setBindCoreidx(bindCoreIdx);
            if (networkRound > 1) {
                es.submit(decodingTask);
            }
            codingSubmitEnd = System.nanoTime();

            transferStart = System.nanoTime();
            for (int i = 0; i < networkRound; i++) {
                subTransferStart[i] = System.nanoTime();
                transferBufferQueue.clear();
                for (CrailBuffer buf : dataBufferQueue) {
                    buf.limit(networkSizeArray.get(i));
                    if (i == 0) {
                        buf.position(0);
                    } else {
                        buf.position(networkSizeArray.get(i - 1));
                    }
                    transferBufferQueue.add(buf);
                }
                ((CoreErasureCodingInputStream) directStream).readSplitsAsync(transferBufferQueue);
                double ret = ((CoreErasureCodingInputStream) directStream).syncResults();
                subTransferEnd[i] = System.nanoTime();

                subUpdateStart[i] = System.nanoTime();
                if (networkRound == 1) {
                    codingLib.NativeDecoding(src, eraseData, srcIndices, degradeIndices, NumDataBlock, NumParityBlock, (int) size, degradeNum, bindCoreIdx);
                } else {
                    cursor_network = networkSizeArray.get(i) / Monec_buffer_split_size;
                    setCounter(bufHasTransfered, cursor_network);

                    hasCoded = getCounter(bufHasCodedBuf);
//                    System.out.println("cursor_network " + cursor_network + " hasCoded " + hasCoded);
                    subHasCoded[i] = hasCoded;
                }
                subUpdateEnd[i] = System.nanoTime();
            }
            transferEnd = System.nanoTime();

            syncStart = System.nanoTime();
            // check decoding is whether finished
            while (networkRound != 1 && !(cursor.get(monECSubStripeNum - 1) == '1')) {
                // blocking until decoding finishing
            }
            syncEnd = System.nanoTime();
            coreCache.releaseCore();

            loopEnd = System.nanoTime();

            everyLoopLatency[ops] = loopEnd - loopStart;
            loopLatencyRecord.get(ops).add(loopEnd - loopStart);
            loopLatencyRecord.get(ops).add((long) bindCoreIdx);
            loopLatencyRecord.get(ops).add(codingSubmitEnd - codingSubmitStart);
            loopLatencyRecord.get(ops).add(transferEnd - transferStart);
            for (int i = 0; i < networkRound; i++) {
                loopLatencyRecord.get(ops).add(subTransferEnd[i] - subTransferStart[i]);
                loopLatencyRecord.get(ops).add(subUpdateEnd[i] - subUpdateStart[i]);
                loopLatencyRecord.get(ops).add((long) subHasCoded[i]);
            }
            loopLatencyRecord.get(ops).add(syncEnd - syncStart);
            ops = ops + 1;
        }

        ((CoreErasureCodingInputStream) directStream).closeSlavesInputStream();
        es.shutdown();

        System.out.println("ops " + ops);
        System.out.println("final loop (us) " + ((double) (loopEnd - loopStart)) / 1000.0);
        getAvgLoopLatency();

        for (int i = 0; i < loopLatencyRecord.size(); i++) {
            System.out.println("Loop " + i + " " + loopLatencyRecord.get(i));
        }

        fs.getStatistics().print("close");
    }

    void warmupDegradeReadMicroECConcurrentTrans(CrailInputStream directStream, int operations, DecodingTask decodingTask, ExecutorService es, ByteBuffer[] src, ByteBuffer[] eraseData, ByteBuffer cursor, int[] srcIndices, ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue, ConcurrentLinkedQueue<CrailBuffer> eraseBufferQueue, ConcurrentLinkedQueue<CrailBuffer> transferBufferQueue, ByteBuffer bufHasTransfered, ByteBuffer bufHasCodedBuf, Crailcoding codingLib, CodingCoreCache coreCache, List<Integer> networkSizeArray) throws Exception {
        int networkRound = networkSizeArray.size();
        int NumDataBlock = decodingTask.NumDataBlock;
        int NumParityBlock = decodingTask.NumParityBlock;
        int size = decodingTask.Microec_buffer_size;
        int monECSubStripeNum = decodingTask.NumSubStripe;
        int Monec_buffer_split_size = size / monECSubStripeNum;
        String fullzero = getFullZeroString(monECSubStripeNum);
        int cursor_network = 0;
        int bindCoreIdx = 11;
        int ops = 0;
        while (ops++ < operations) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            clearBufferOfConcurrentLinkedQueue(eraseBufferQueue);
            setByteBuffer(cursor, fullzero);
            cursor_network = 0;
            zeroByteBuffer(bufHasTransfered);
            zeroByteBuffer(bufHasCodedBuf);
            ((CoreErasureCodingInputStream) directStream).resetStreams();

            // start decoding process
            bindCoreIdx = coreCache.randomGetFreeCoreIdx();
            decodingTask.setDecodingBuffer(src, eraseData, cursor);
            decodingTask.setCodingCounter(bufHasTransfered, bufHasCodedBuf);
            decodingTask.setBindCoreidx(bindCoreIdx);
            if (networkRound > 1) {
                es.submit(decodingTask);
            }

            for (int i = 0; i < networkRound; i++) {
                transferBufferQueue.clear();
                for (CrailBuffer buf : dataBufferQueue) {
                    buf.limit(networkSizeArray.get(i));
                    if (i == 0) {
                        buf.position(0);
                    } else {
                        buf.position(networkSizeArray.get(i - 1));
                    }
                    transferBufferQueue.add(buf);
                }
                ((CoreErasureCodingInputStream) directStream).readSplitsAsync(transferBufferQueue);
            }

            for (int i = 0; i < networkRound; i++) {
                double ret = ((CoreErasureCodingInputStream) directStream).syncResults(NumDataBlock);
                if (networkRound == 1) {
                    codingLib.NativeDecoding(src, eraseData, srcIndices, degradeIndices, NumDataBlock, NumParityBlock, (int) size, degradeNum, bindCoreIdx);
                } else {
                    cursor_network = networkSizeArray.get(i) / Monec_buffer_split_size;
                    setCounter(bufHasTransfered, cursor_network);
                }
            }
            // check decoding is whether finished
            while (networkRound != 1 && !(cursor.get(monECSubStripeNum - 1) == '1')) {
                // blocking until decoding finishing
            }
            coreCache.releaseCore();
        }
    }

    // microec pipeline degrade read
    void degradedReadMicroECConcurrentTrans(String filename, int monECSubStripeNum, int transferSize, int loop) throws Exception {
        System.out.println("degradedReadMicroECConcurrentTrans, filename " + filename + " monECSubStripeNum " + monECSubStripeNum + " transferSize " + transferSize + " loop " + loop + " degradeNum " + degradeNum);

//        if (!CrailConstants.REDUNDANCY_TYPE.equals("erasurecoding")) {
//            throw new Exception("CrailConstants.REDUNDANCY_TYPE is not equal to erasure coding!");
//        }
        CrailConstants.REPLICATION_FACTOR = CrailConstants.ERASURECODING_K + CrailConstants.ERASURECODING_M - 1;
        int NumDataBlock = CrailConstants.ERASURECODING_K;
        int NumParityBlock = CrailConstants.ERASURECODING_M;
        Crailcoding codingLib = new Crailcoding(NumDataBlock, NumParityBlock);
        CodingCoreCache coreCache = new CodingCoreCache();
        int bindCoreIdx = 11;

        // test, one straggler degrade read
        int[] srcIndices = getDegradeSrcIndices(degradeIndices);

        CrailFile file = fs.lookup(filename).get().asFile();
        printFileType(file);

        // allocate buffer for storing read data
        // single split size
        long size = file.getCapacity();
        // pure ec split size
        int Microec_buffer_size = (int) size;
        // monec encoding size
        int Monec_buffer_split_size = Microec_buffer_size / monECSubStripeNum;
        // network transfer size list
        List<Integer> networkSizeArray = getDegradeReadNetworkSize(Microec_buffer_size, transferSize);
        int networkRound = networkSizeArray.size();
        System.out.println("networkSizeArray " + networkSizeArray);

        // store read src data
        ByteBuffer[] src = new ByteBuffer[NumDataBlock];
        ByteBuffer[] eraseData = new ByteBuffer[degradeNum];
        ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        ConcurrentLinkedQueue<CrailBuffer> eraseBufferQueue = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<CrailBuffer> transferBufferQueue = new ConcurrentLinkedQueue<>();
        allocateDataBuffers(src, Microec_buffer_size, dataBufferQueue);
        allocateParityBuffers(eraseData, Microec_buffer_size, eraseBufferQueue);
        ByteBuffer cursor = ByteBuffer.allocateDirect(monECSubStripeNum);
        ByteBuffer bufHasTransfered = ByteBuffer.allocateDirect(2);
        ByteBuffer bufHasCodedBuf = ByteBuffer.allocateDirect(2);
        String fullzero = getFullZeroString(monECSubStripeNum);
        setByteBuffer(cursor, fullzero);
        // transfer buffer
        int cursor_network = 0, hasCoded = 0;
        CrailInputStream directStream = ((ReplicasFile) file).getDegradeErasureCodingDirectInputStream(Microec_buffer_size, degradeIndices);

        DecodingTask decodingTask = new DecodingTask(codingLib, monECSubStripeNum, NumDataBlock, NumParityBlock, Microec_buffer_size, srcIndices, degradeIndices, degradeNum);
        ExecutorService es = Executors.newSingleThreadExecutor();

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        int ops = 0;
        everyLoopLatency = new long[loop];
        long loopStart = 0, loopEnd = 0;
        long codingSubmitStart, codingSubmitEnd, readSubmitStart, readSubmitEnd, transferStart, transferEnd, syncStart, syncEnd;
        long[] subTransferStart = new long[networkRound];
        long[] subTransferEnd = new long[networkRound];
        long[] subUpdateStart = new long[networkRound];
        long[] subUpdateEnd = new long[networkRound];
        int[] subHasCoded = new int[networkRound];
        List<List<Long>> loopLatencyRecord = new ArrayList<>();
        for (int i = 0; i < loop; i++) {
            loopLatencyRecord.add(new ArrayList<>());
        }

        warmupDegradeReadMicroECConcurrentTrans(directStream, loop, decodingTask, es, src, eraseData, cursor, srcIndices, dataBufferQueue, eraseBufferQueue, transferBufferQueue, bufHasTransfered, bufHasCodedBuf, codingLib, coreCache, networkSizeArray);
        while (ops < loop) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            clearBufferOfConcurrentLinkedQueue(eraseBufferQueue);
            setByteBuffer(cursor, fullzero);
            cursor_network = 0;
            hasCoded = 0;
            zeroByteBuffer(bufHasTransfered);
            zeroByteBuffer(bufHasCodedBuf);
            ((CoreErasureCodingInputStream) directStream).resetStreams();

//            System.out.println("ops "+ops);
            loopStart = System.nanoTime();
            codingSubmitStart = System.nanoTime();
            // start decoding process
            bindCoreIdx = coreCache.randomGetFreeCoreIdx();
            decodingTask.setDecodingBuffer(src, eraseData, cursor);
            decodingTask.setCodingCounter(bufHasTransfered, bufHasCodedBuf);
            decodingTask.setBindCoreidx(bindCoreIdx);
            if (networkRound > 1) {
                es.submit(decodingTask);
            }
            codingSubmitEnd = System.nanoTime();

            readSubmitStart = System.nanoTime();
            for (int i = 0; i < networkRound; i++) {
                transferBufferQueue.clear();
                for (CrailBuffer buf : dataBufferQueue) {
                    buf.limit(networkSizeArray.get(i));
                    if (i == 0) {
                        buf.position(0);
                    } else {
                        buf.position(networkSizeArray.get(i - 1));
                    }
                    transferBufferQueue.add(buf);
                }
                ((CoreErasureCodingInputStream) directStream).readSplitsAsync(transferBufferQueue);
            }
            readSubmitEnd = System.nanoTime();

            transferStart = System.nanoTime();
            for (int i = 0; i < networkRound; i++) {
                subTransferStart[i] = System.nanoTime();
                double ret = ((CoreErasureCodingInputStream) directStream).syncResults(NumDataBlock);
                subTransferEnd[i] = System.nanoTime();

                subUpdateStart[i] = System.nanoTime();
                if (networkRound == 1) {
                    codingLib.NativeDecoding(src, eraseData, srcIndices, degradeIndices, NumDataBlock, NumParityBlock, (int) size, degradeNum, bindCoreIdx);
                } else {
                    cursor_network = networkSizeArray.get(i) / Monec_buffer_split_size;
                    setCounter(bufHasTransfered, cursor_network);

                    hasCoded = getCounter(bufHasCodedBuf);
//                    System.out.println("cursor_network " + cursor_network + " hasCoded " + hasCoded);
                    subHasCoded[i] = hasCoded;
                }
                subUpdateEnd[i] = System.nanoTime();
            }
            transferEnd = System.nanoTime();

            syncStart = System.nanoTime();
            // check decoding is whether finished
            while (networkRound != 1 && !(cursor.get(monECSubStripeNum - 1) == '1')) {
                // blocking until decoding finishing
            }
            syncEnd = System.nanoTime();
            coreCache.releaseCore();

            loopEnd = System.nanoTime();

            everyLoopLatency[ops] = loopEnd - loopStart;
            loopLatencyRecord.get(ops).add(loopEnd - loopStart);
            loopLatencyRecord.get(ops).add((long) bindCoreIdx);
            loopLatencyRecord.get(ops).add(codingSubmitEnd - codingSubmitStart);
            loopLatencyRecord.get(ops).add(readSubmitEnd - readSubmitStart);
            loopLatencyRecord.get(ops).add(transferEnd - transferStart);
            for (int i = 0; i < networkRound; i++) {
                loopLatencyRecord.get(ops).add(subTransferEnd[i] - subTransferStart[i]);
                loopLatencyRecord.get(ops).add(subUpdateEnd[i] - subUpdateStart[i]);
                loopLatencyRecord.get(ops).add((long) subHasCoded[i]);
            }
            loopLatencyRecord.get(ops).add(syncEnd - syncStart);
            ops = ops + 1;
        }

        ((CoreErasureCodingInputStream) directStream).closeSlavesInputStream();
        es.shutdown();

        System.out.println("ops " + ops);
        System.out.println("final loop (us) " + ((double) (loopEnd - loopStart)) / 1000.0);
        getAvgLoopLatency();

        for (int i = 0; i < loopLatencyRecord.size(); i++) {
            System.out.println("Loop " + i + " " + loopLatencyRecord.get(i));
        }

        fs.getStatistics().print("close");
    }

    void warmupRecoveryMicroEC(CrailInputStream directStream, int operations, DecodingTask decodingTask, ExecutorService es, ByteBuffer[] src, ByteBuffer[] eraseData, ByteBuffer cursor, int[] srcIndices, ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue, ConcurrentLinkedQueue<CrailBuffer> eraseBufferQueue, ConcurrentLinkedQueue<CrailBuffer> transferBufferQueue, ByteBuffer bufHasTransfered, ByteBuffer bufHasCodedBuf, List<CrailOutputStream> recoverOutputStreams, LinkedBlockingQueue<Future<CrailResult>> futureQueue, Crailcoding codingLib, CodingCoreCache coreCache, List<Integer> networkSizeArray) throws Exception {
        int networkRound = networkSizeArray.size();
        int NumDataBlock = decodingTask.NumDataBlock;
        int NumParityBlock = decodingTask.NumParityBlock;
        int size = decodingTask.Microec_buffer_size;
        int monECSubStripeNum = decodingTask.NumSubStripe;
        int Monec_buffer_split_size = size / monECSubStripeNum;
        String fullzero = getFullZeroString(monECSubStripeNum);
        int cursor_network = 0, hasCoded = 0, hasRecovered = 0;
        long blockingStart, blockingEnd;
        int bindCoreIdx = 11;
        int ops = 0;
        while (ops++ < operations) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            clearBufferOfConcurrentLinkedQueue(eraseBufferQueue);
            setByteBuffer(cursor, fullzero);
            cursor_network = 0;
            hasCoded = 0;
            hasRecovered = 0;
            zeroByteBuffer(bufHasTransfered);
            zeroByteBuffer(bufHasCodedBuf);
            ((CoreErasureCodingInputStream) directStream).resetStreams();
            for (int i = 0; i < degradeNum; i++) {
                ((CoreOutputStream) recoverOutputStreams.get(i)).seek(0);
            }

//            System.out.println("ops " + ops);
            // start decoding process
            bindCoreIdx = coreCache.randomGetFreeCoreIdx();
            decodingTask.setDecodingBuffer(src, eraseData, cursor);
            decodingTask.setCodingCounter(bufHasTransfered, bufHasCodedBuf);
            decodingTask.setBindCoreidx(bindCoreIdx);
            if (networkRound > 1) {
                es.submit(decodingTask);
            }

            for (int i = 0; i < networkRound; i++) {
                transferBufferQueue.clear();
                for (CrailBuffer buf : dataBufferQueue) {
                    buf.limit(networkSizeArray.get(i));
                    if (i == 0) {
                        buf.position(0);
                    } else {
                        buf.position(networkSizeArray.get(i - 1));
                    }
                    transferBufferQueue.add(buf);
                }
                ((CoreErasureCodingInputStream) directStream).readSplitsAsync(transferBufferQueue);
                double ret = ((CoreErasureCodingInputStream) directStream).syncResults();

                int j = 0;
                if (networkRound == 1) {
                    codingLib.NativeDecoding(src, eraseData, srcIndices, degradeIndices, NumDataBlock, NumParityBlock, (int) size, degradeNum, bindCoreIdx);

                    for (CrailBuffer buf : eraseBufferQueue) {
                        futureQueue.add(recoverOutputStreams.get(j++).write(buf));
                    }
                } else {
                    cursor_network = networkSizeArray.get(i) / Monec_buffer_split_size;
                    setCounter(bufHasTransfered, cursor_network);
                    hasRecovered = hasCoded;
                    hasCoded = getCounter(bufHasCodedBuf);

                    // hasCoded may be error read (> cursor_network)
                    // solve: retry read until hasCoded <= cursor_network
                    // use optimistic lock to avoid read the modifying hasCoded
                    blockingStart = System.nanoTime();
                    while(hasCoded > cursor_network || hasCoded * Monec_buffer_split_size < hasRecovered) {
                        // blocking until read the collect hasCoded
                        hasCoded = getCounter(bufHasCodedBuf);

                        blockingEnd = System.nanoTime();
                        if(blockingEnd - blockingStart > blockingBound) {
                            System.out.println("while loop warmupRecoveryMicroEC getCounter(bufHasCodedBuf) " + getCounter(bufHasCodedBuf) + " cursor_network " + cursor_network + " hasRecovered " + hasRecovered + " hasCoded " + hasCoded);
                            throw new Exception("warmupRecoveryMicroEC hasCoded error! getCounter(bufHasCodedBuf) " + getCounter(bufHasCodedBuf) + " cursor_network " + cursor_network + " hasRecovered " + hasRecovered + " hasCoded " + hasCoded);
                        }
                    }

//                    System.out.println("cursor_network " + cursor_network + " hasRecovered " + hasRecovered + " hasCoded "+hasCoded);
                    hasCoded *= Monec_buffer_split_size;
                    if (hasRecovered != hasCoded) {
//                        System.out.println("write buf range " + hasRecovered + " " + hasCoded);
                        for (CrailBuffer buf : eraseBufferQueue) {
                            buf.limit(hasCoded);
                            buf.position(hasRecovered);
                            CrailBuffer tmp = buf.slice();
                            futureQueue.add(recoverOutputStreams.get(j++).write(tmp));
                        }
                    }
                }
            }

            // check decoding is whether finished
            while (networkRound != 1 && !(cursor.get(monECSubStripeNum - 1) == '1')) {
                // blocking until decoding finishing
            }
            coreCache.releaseCore();

            if (networkRound != 1) {
                int j = 0;
                for (CrailBuffer buf : eraseBufferQueue) {
                    buf.limit(size);
                    buf.position(hasCoded);
                    CrailBuffer tmp = buf.slice();
                    futureQueue.add(recoverOutputStreams.get(j++).write(tmp));
                }
            }

            while (!futureQueue.isEmpty()) {
                Future<CrailResult> future = futureQueue.poll();
                future.get();
            }
        }
    }

    // microec pipeline recovery
    void recoveryMicroEC(String filename, int monECSubStripeNum, int transferSize, int loop) throws Exception {
        System.out.println("recoveryMicroEC, filename " + filename + " monECSubStripeNum " + monECSubStripeNum + " transferSize " + transferSize + " loop " + loop + " degradeNum " + degradeNum);

//        if (!CrailConstants.REDUNDANCY_TYPE.equals("erasurecoding")) {
//            throw new Exception("CrailConstants.REDUNDANCY_TYPE is not equal to erasure coding!");
//        }
        CrailConstants.REPLICATION_FACTOR = CrailConstants.ERASURECODING_K + CrailConstants.ERASURECODING_M - 1;
        int NumDataBlock = CrailConstants.ERASURECODING_K;
        int NumParityBlock = CrailConstants.ERASURECODING_M;
        Crailcoding codingLib = new Crailcoding(NumDataBlock, NumParityBlock);
        CodingCoreCache coreCache = new CodingCoreCache();
        int bindCoreIdx = 11;

        everyLoopLatency = new long[loop];

        // test, one straggler degrade read
        int[] srcIndices = getDegradeSrcIndices(degradeIndices);

        CrailFile file = fs.lookup(filename).get().asFile();
        printFileType(file);

        // allocate buffer for storing read data
        // single split size
        long size = file.getCapacity();
        // pure ec split size
        int Microec_buffer_size = (int) size;
        // monec encoding size
        int Monec_buffer_split_size = Microec_buffer_size / monECSubStripeNum;
        // network transfer size list
        List<Integer> networkSizeArray = getDegradeReadNetworkSize(Microec_buffer_size, transferSize);
        int networkRound = networkSizeArray.size();
        System.out.println("networkSizeArray " + networkSizeArray);

        // store read src data
        ByteBuffer[] src = new ByteBuffer[NumDataBlock];
        ByteBuffer[] eraseData = new ByteBuffer[degradeNum];
        ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        ConcurrentLinkedQueue<CrailBuffer> eraseBufferQueue = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<CrailBuffer> transferBufferQueue = new ConcurrentLinkedQueue<>();
        allocateDataBuffers(src, Microec_buffer_size, dataBufferQueue);
        allocateParityBuffers(eraseData, Microec_buffer_size, eraseBufferQueue);
        ByteBuffer cursor = ByteBuffer.allocateDirect(monECSubStripeNum);
        ByteBuffer bufHasTransfered = ByteBuffer.allocateDirect(2);
        ByteBuffer bufHasCodedBuf = ByteBuffer.allocateDirect(2);
        String fullzero = getFullZeroString(monECSubStripeNum);
        setByteBuffer(cursor, fullzero);
        // transfer buffer
        int cursor_network = 0, hasCoded = 0, hasRecovered = 0;
        CrailInputStream directStream = ((ReplicasFile) file).getDegradeErasureCodingDirectInputStream(Microec_buffer_size, degradeIndices);

        // create a recovery file
        List<CrailFile> recoverFiles = new ArrayList<>();
        List<CrailOutputStream> recoverOutputStreams = new ArrayList<>();
        LinkedBlockingQueue<Future<CrailResult>> futureQueue = new LinkedBlockingQueue<>();
        for (int i = 0; i < degradeNum; i++) {
            CrailFile rf = fs.create(filename + transferSize + ".microecrec" + i, file.getType(), CrailStorageClass.get(0), CrailLocationClass.get(0), !false).get().asFile();
            CrailOutputStream recoverDirectStream = rf.getDirectOutputStream(size);
            recoverFiles.add(rf);
            recoverOutputStreams.add(recoverDirectStream);
        }

        DecodingTask decodingTask = new DecodingTask(codingLib, monECSubStripeNum, NumDataBlock, NumParityBlock, Microec_buffer_size, srcIndices, degradeIndices, degradeNum);
        ExecutorService es = Executors.newSingleThreadExecutor();

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        int ops = 0;
        long loopStart = 0, loopEnd = 0;
        long codingSubmitStart, codingSubmitEnd, transferStart, transferEnd, syncCodingStart, syncCodingEnd, syncTransferStart, syncTransferEnd, blockingStart, blockingEnd;
        long[] subTransferStart = new long[networkRound];
        long[] subTransferEnd = new long[networkRound];
        long[] subUpdateStart = new long[networkRound];
        long[] subUpdateEnd = new long[networkRound];
        int[] subHasCoded = new int[networkRound];
        List<List<Long>> loopLatencyRecord = new ArrayList<>();
        for (int i = 0; i < loop; i++) {
            loopLatencyRecord.add(new ArrayList<>());
        }

        warmupRecoveryMicroEC(directStream, loop, decodingTask, es, src, eraseData, cursor, srcIndices, dataBufferQueue, eraseBufferQueue, transferBufferQueue, bufHasTransfered, bufHasCodedBuf, recoverOutputStreams, futureQueue, codingLib, coreCache, networkSizeArray);
        while (ops < loop) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            clearBufferOfConcurrentLinkedQueue(eraseBufferQueue);
            setByteBuffer(cursor, fullzero);
            cursor_network = 0;
            hasCoded = 0;
            hasRecovered = 0;
            zeroByteBuffer(bufHasTransfered);
            zeroByteBuffer(bufHasCodedBuf);
            ((CoreErasureCodingInputStream) directStream).resetStreams();
            for (int i = 0; i < degradeNum; i++) {
                ((CoreOutputStream) recoverOutputStreams.get(i)).seek(0);
            }

//            System.out.println("recovery ops " + ops);
            loopStart = System.nanoTime();
            codingSubmitStart = System.nanoTime();
            // start decoding process
//            bindCoreIdx = coreCache.randomGetFreeCoreIdx();
//            bindCoreIdx = coreCache.getFreeCoreIdx();
            bindCoreIdx = size > maxSmallObjSize ? coreCache.randomGetFreeCoreIdx() : coreCache.getFreeCoreIdx();
            decodingTask.setDecodingBuffer(src, eraseData, cursor);
            decodingTask.setCodingCounter(bufHasTransfered, bufHasCodedBuf);
            decodingTask.setBindCoreidx(bindCoreIdx);
            if (networkRound > 1) {
                es.submit(decodingTask);
            }
            codingSubmitEnd = System.nanoTime();

            transferStart = System.nanoTime();
            for (int i = 0; i < networkRound; i++) {
                subTransferStart[i] = System.nanoTime();
                transferBufferQueue.clear();
                for (CrailBuffer buf : dataBufferQueue) {
                    buf.limit(networkSizeArray.get(i));
                    if (i == 0) {
                        buf.position(0);
                    } else {
                        buf.position(networkSizeArray.get(i - 1));
                    }
                    transferBufferQueue.add(buf);
                }
                ((CoreErasureCodingInputStream) directStream).readSplitsAsync(transferBufferQueue);
                double ret = ((CoreErasureCodingInputStream) directStream).syncResults();
                subTransferEnd[i] = System.nanoTime();

                subUpdateStart[i] = System.nanoTime();
                int j = 0;
                if (networkRound == 1) {
                    codingLib.NativeDecoding(src, eraseData, srcIndices, degradeIndices, NumDataBlock, NumParityBlock, (int) size, degradeNum, bindCoreIdx);

                    for (CrailBuffer buf : eraseBufferQueue) {
                        futureQueue.add(recoverOutputStreams.get(j++).write(buf));
                    }
                } else {
                    cursor_network = networkSizeArray.get(i) / Monec_buffer_split_size;
                    setCounter(bufHasTransfered, cursor_network);
                    hasRecovered = hasCoded;
                    hasCoded = getCounter(bufHasCodedBuf);

                    // hasCoded may be error read (> cursor_network)
                    // solve: retry read until hasCoded <= cursor_network
                    // use optimistic lock to avoid read the modifying hasCoded
                    blockingStart = System.nanoTime();
                    while(hasCoded > cursor_network || hasCoded * Monec_buffer_split_size < hasRecovered) {
                        // blocking until read the collect hasCoded
                        hasCoded = getCounter(bufHasCodedBuf);

                        blockingEnd = System.nanoTime();
                        if(blockingEnd - blockingStart > blockingBound) {
                            System.out.println("while loop recoveryMicroEC getCounter(bufHasCodedBuf) " + getCounter(bufHasCodedBuf) + " cursor_network " + cursor_network + " hasRecovered " + hasRecovered + " hasCoded " + hasCoded);
                            throw new Exception("recoveryMicroEC hasCoded error! getCounter(bufHasCodedBuf) " + getCounter(bufHasCodedBuf) + " cursor_network " + cursor_network + " hasRecovered " + hasRecovered + " hasCoded " + hasCoded);
                        }
                    }

                    subHasCoded[i] = hasCoded;
//                    System.out.println("recovery cursor_network " + cursor_network + " hasRecovered " + hasRecovered + " hasCoded "+hasCoded);

                    hasCoded *= Monec_buffer_split_size;
                    if (hasRecovered != hasCoded) {
//                        System.out.println("recovery write buf range " + hasRecovered + " " + hasCoded);
                        for (CrailBuffer buf : eraseBufferQueue) {
                            buf.limit(hasCoded);
                            buf.position(hasRecovered);
                            CrailBuffer tmp = buf.slice();
                            futureQueue.add(recoverOutputStreams.get(j++).write(tmp));
                        }
                    }
                }
                subUpdateEnd[i] = System.nanoTime();
            }
            transferEnd = System.nanoTime();

            syncCodingStart = System.nanoTime();
            // check decoding is whether finished
            while (networkRound != 1 && !(cursor.get(monECSubStripeNum - 1) == '1')) {
                // blocking until decoding finishing
            }
            syncCodingEnd = System.nanoTime();
            coreCache.releaseCore();

            syncTransferStart = System.nanoTime();
            if (networkRound != 1) {
                int j = 0;
                for (CrailBuffer buf : eraseBufferQueue) {
                    buf.limit(Microec_buffer_size);
                    buf.position(hasCoded);
                    CrailBuffer tmp = buf.slice();
                    futureQueue.add(recoverOutputStreams.get(j++).write(tmp));
                }
            }
//            System.out.println("final range " + hasCoded + " " + Microec_buffer_size);

            while (!futureQueue.isEmpty()) {
                Future<CrailResult> future = futureQueue.poll();
                future.get();
            }
            syncTransferEnd = System.nanoTime();
            loopEnd = System.nanoTime();

            everyLoopLatency[ops] = loopEnd - loopStart;
            loopLatencyRecord.get(ops).add(loopEnd - loopStart);
            loopLatencyRecord.get(ops).add((long) bindCoreIdx);
            loopLatencyRecord.get(ops).add(codingSubmitEnd - codingSubmitStart);
            loopLatencyRecord.get(ops).add(transferEnd - transferStart);
            for (int i = 0; i < networkRound; i++) {
                loopLatencyRecord.get(ops).add(subTransferEnd[i] - subTransferStart[i]);
                loopLatencyRecord.get(ops).add(subUpdateEnd[i] - subUpdateStart[i]);
                loopLatencyRecord.get(ops).add((long) subHasCoded[i]);
            }
            loopLatencyRecord.get(ops).add(syncCodingEnd - syncCodingStart);
            loopLatencyRecord.get(ops).add(syncTransferEnd - syncTransferStart);
            ops = ops + 1;
        }

        ((CoreErasureCodingInputStream) directStream).closeSlavesInputStream();
        for (int i = 0; i < degradeNum; i++) {
            recoverOutputStreams.get(i).close();
        }
        es.shutdown();

        System.out.println("ops " + ops);
        System.out.println("final loop (us) " + ((double) (loopEnd - loopStart)) / 1000.0);
        getAvgLoopLatency();

        for (int i = 0; i < loopLatencyRecord.size(); i++) {
            System.out.println("Loop " + i + " " + loopLatencyRecord.get(i));
        }
        getRedundantFileCapacity(recoverFiles);

        fs.getStatistics().print("close");
    }

    void warmupRecoveryMicroECConcurrentTrans(CrailInputStream directStream, int operations, DecodingTask decodingTask, ExecutorService es, ByteBuffer[] src, ByteBuffer[] eraseData, ByteBuffer cursor, int[] srcIndices, ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue, ConcurrentLinkedQueue<CrailBuffer> eraseBufferQueue, ConcurrentLinkedQueue<CrailBuffer> transferBufferQueue, ByteBuffer bufHasTransfered, ByteBuffer bufHasCodedBuf, List<CrailOutputStream> recoverOutputStreams, LinkedBlockingQueue<Future<CrailResult>> futureQueue, Crailcoding codingLib, CodingCoreCache coreCache, List<Integer> networkSizeArray) throws Exception {
        int networkRound = networkSizeArray.size();
        int NumDataBlock = decodingTask.NumDataBlock;
        int NumParityBlock = decodingTask.NumParityBlock;
        int size = decodingTask.Microec_buffer_size;
        int monECSubStripeNum = decodingTask.NumSubStripe;
        int Monec_buffer_split_size = size / monECSubStripeNum;
        String fullzero = getFullZeroString(monECSubStripeNum);
        int cursor_network = 0, hasCoded = 0, hasRecovered = 0;
        long blockingStart, blockingEnd;
        int bindCoreIdx = 11;
        int ops = 0;
        while (ops++ < operations) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            clearBufferOfConcurrentLinkedQueue(eraseBufferQueue);
            setByteBuffer(cursor, fullzero);
            cursor_network = 0;
            hasCoded = 0;
            hasRecovered = 0;
            zeroByteBuffer(bufHasTransfered);
            zeroByteBuffer(bufHasCodedBuf);
            ((CoreErasureCodingInputStream) directStream).resetStreams();
            for (int i = 0; i < degradeNum; i++) {
                ((CoreOutputStream) recoverOutputStreams.get(i)).seek(0);
            }

            // start decoding process
            bindCoreIdx = coreCache.randomGetFreeCoreIdx();
            decodingTask.setDecodingBuffer(src, eraseData, cursor);
            decodingTask.setCodingCounter(bufHasTransfered, bufHasCodedBuf);
            decodingTask.setBindCoreidx(bindCoreIdx);
            if (networkRound > 1) {
                es.submit(decodingTask);
            }

            for (int i = 0; i < networkRound; i++) {
                transferBufferQueue.clear();
                for (CrailBuffer buf : dataBufferQueue) {
                    buf.limit(networkSizeArray.get(i));
                    if (i == 0) {
                        buf.position(0);
                    } else {
                        buf.position(networkSizeArray.get(i - 1));
                    }
                    transferBufferQueue.add(buf);
                }
                ((CoreErasureCodingInputStream) directStream).readSplitsAsync(transferBufferQueue);
            }

            for (int i = 0; i < networkRound; i++) {
                double ret = ((CoreErasureCodingInputStream) directStream).syncResults(NumDataBlock);
                int j = 0;
                if (networkRound == 1) {
                    codingLib.NativeDecoding(src, eraseData, srcIndices, degradeIndices, NumDataBlock, NumParityBlock, (int) size, degradeNum, bindCoreIdx);

                    for (CrailBuffer buf : eraseBufferQueue) {
                        futureQueue.add(recoverOutputStreams.get(j++).write(buf));
                    }
                } else {
                    cursor_network = networkSizeArray.get(i) / Monec_buffer_split_size;
                    setCounter(bufHasTransfered, cursor_network);
                    hasRecovered = hasCoded;
                    hasCoded = getCounter(bufHasCodedBuf);

                    // hasCoded may be error read (> cursor_network)
                    // solve: retry read until hasCoded <= cursor_network
                    // use optimistic lock to avoid read the modifying hasCoded
                    blockingStart = System.nanoTime();
                    while(hasCoded > cursor_network || hasCoded * Monec_buffer_split_size < hasRecovered) {
                        // blocking until read the collect hasCoded
                        hasCoded = getCounter(bufHasCodedBuf);

                        blockingEnd = System.nanoTime();
                        if(blockingEnd - blockingStart > blockingBound) {
                            System.out.println("while loop warmupRecoveryMicroECConcurrentTrans getCounter(bufHasCodedBuf) " + getCounter(bufHasCodedBuf) + " cursor_network " + cursor_network + " hasRecovered " + hasRecovered + " hasCoded " + hasCoded);
                            throw new Exception("warmupRecoveryMicroECConcurrentTrans hasCoded error! getCounter(bufHasCodedBuf) " + getCounter(bufHasCodedBuf) + " cursor_network " + cursor_network + " hasRecovered " + hasRecovered + " hasCoded " + hasCoded);
                        }
                    }

                    hasCoded *= Monec_buffer_split_size;
                    if (hasRecovered != hasCoded) {
                        for (CrailBuffer buf : eraseBufferQueue) {
                            buf.limit(hasCoded);
                            buf.position(hasRecovered);
                            CrailBuffer tmp = buf.slice();
                            futureQueue.add(recoverOutputStreams.get(j++).write(tmp));
                        }
                    }
                }
            }
            // check decoding is whether finished
            while (networkRound != 1 && !(cursor.get(monECSubStripeNum - 1) == '1')) {
                // blocking until decoding finishing
            }
            coreCache.releaseCore();

            if (networkRound != 1) {
                int j = 0;
                for (CrailBuffer buf : eraseBufferQueue) {
                    buf.limit(size);
                    buf.position(hasCoded);
                    CrailBuffer tmp = buf.slice();
                    futureQueue.add(recoverOutputStreams.get(j++).write(tmp));
                }
            }

            while (!futureQueue.isEmpty()) {
                Future<CrailResult> future = futureQueue.poll();
                future.get();
            }
        }
    }

    // microec pipeline recovery
    void recoveryMicroECConcurrentTrans(String filename, int monECSubStripeNum, int transferSize, int loop) throws Exception {
        System.out.println("recoveryMicroECConcurrentTrans, filename " + filename + " monECSubStripeNum " + monECSubStripeNum + " transferSize " + transferSize + " loop " + loop + " degradeNum " + degradeNum);

//        if (!CrailConstants.REDUNDANCY_TYPE.equals("erasurecoding")) {
//            throw new Exception("CrailConstants.REDUNDANCY_TYPE is not equal to erasure coding!");
//        }
        CrailConstants.REPLICATION_FACTOR = CrailConstants.ERASURECODING_K + CrailConstants.ERASURECODING_M - 1;
        int NumDataBlock = CrailConstants.ERASURECODING_K;
        int NumParityBlock = CrailConstants.ERASURECODING_M;
        Crailcoding codingLib = new Crailcoding(NumDataBlock, NumParityBlock);
        CodingCoreCache coreCache = new CodingCoreCache();
        int bindCoreIdx = 11;

        everyLoopLatency = new long[loop];

        // test, one straggler degrade read
        int[] srcIndices = getDegradeSrcIndices(degradeIndices);

        CrailFile file = fs.lookup(filename).get().asFile();
        printFileType(file);

        // allocate buffer for storing read data
        // single split size
        long size = file.getCapacity();
        // pure ec split size
        int Microec_buffer_size = (int) size;
        // monec encoding size
        int Monec_buffer_split_size = Microec_buffer_size / monECSubStripeNum;
        // network transfer size list
        List<Integer> networkSizeArray = getDegradeReadNetworkSize(Microec_buffer_size, transferSize);
        int networkRound = networkSizeArray.size();
        System.out.println("networkSizeArray " + networkSizeArray);

        // store read src data
        ByteBuffer[] src = new ByteBuffer[NumDataBlock];
        ByteBuffer[] eraseData = new ByteBuffer[degradeNum];
        ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        ConcurrentLinkedQueue<CrailBuffer> eraseBufferQueue = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<CrailBuffer> transferBufferQueue = new ConcurrentLinkedQueue<>();
        allocateDataBuffers(src, Microec_buffer_size, dataBufferQueue);
        allocateParityBuffers(eraseData, Microec_buffer_size, eraseBufferQueue);
        ByteBuffer cursor = ByteBuffer.allocateDirect(monECSubStripeNum);
        ByteBuffer bufHasTransfered = ByteBuffer.allocateDirect(2);
        ByteBuffer bufHasCodedBuf = ByteBuffer.allocateDirect(2);
        String fullzero = getFullZeroString(monECSubStripeNum);
        setByteBuffer(cursor, fullzero);
        // transfer buffer
        int cursor_network = 0, hasCoded = 0, hasRecovered = 0;
        CrailInputStream directStream = ((ReplicasFile) file).getDegradeErasureCodingDirectInputStream(Microec_buffer_size, degradeIndices);

        // create a recovery file
        List<CrailFile> recoverFiles = new ArrayList<>();
        List<CrailOutputStream> recoverOutputStreams = new ArrayList<>();
        LinkedBlockingQueue<Future<CrailResult>> futureQueue = new LinkedBlockingQueue<>();
        for (int i = 0; i < degradeNum; i++) {
            CrailFile rf = fs.create(filename + transferSize + ".microecrec" + i, file.getType(), CrailStorageClass.get(0), CrailLocationClass.get(0), !false).get().asFile();
            CrailOutputStream recoverDirectStream = rf.getDirectOutputStream(size);
            recoverFiles.add(rf);
            recoverOutputStreams.add(recoverDirectStream);
        }

        DecodingTask decodingTask = new DecodingTask(codingLib, monECSubStripeNum, NumDataBlock, NumParityBlock, Microec_buffer_size, srcIndices, degradeIndices, degradeNum);
        ExecutorService es = Executors.newSingleThreadExecutor();

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        int ops = 0;
        long loopStart = 0, loopEnd = 0;
        long codingSubmitStart, codingSubmitEnd, readSubmitStart, readSubmitEnd, transferStart, transferEnd, syncCodingStart, syncCodingEnd, syncTransferStart, syncTransferEnd, blockingStart, blockingEnd;
        long[] subTransferStart = new long[networkRound];
        long[] subTransferEnd = new long[networkRound];
        long[] subUpdateStart = new long[networkRound];
        long[] subUpdateEnd = new long[networkRound];
        int[] subHasCoded = new int[networkRound];
        List<List<Long>> loopLatencyRecord = new ArrayList<>();
        for (int i = 0; i < loop; i++) {
            loopLatencyRecord.add(new ArrayList<>());
        }

        warmupRecoveryMicroECConcurrentTrans(directStream, loop, decodingTask, es, src, eraseData, cursor, srcIndices, dataBufferQueue, eraseBufferQueue, transferBufferQueue, bufHasTransfered, bufHasCodedBuf, recoverOutputStreams, futureQueue, codingLib, coreCache, networkSizeArray);
        while (ops < loop) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            clearBufferOfConcurrentLinkedQueue(eraseBufferQueue);
            setByteBuffer(cursor, fullzero);
            cursor_network = 0;
            hasCoded = 0;
            hasRecovered = 0;
            zeroByteBuffer(bufHasTransfered);
            zeroByteBuffer(bufHasCodedBuf);
            ((CoreErasureCodingInputStream) directStream).resetStreams();
            for (int i = 0; i < degradeNum; i++) {
                ((CoreOutputStream) recoverOutputStreams.get(i)).seek(0);
            }

//            System.out.println("recovery ops " + ops);
            loopStart = System.nanoTime();
            codingSubmitStart = System.nanoTime();
            // start decoding process
            bindCoreIdx = coreCache.randomGetFreeCoreIdx();
            decodingTask.setDecodingBuffer(src, eraseData, cursor);
            decodingTask.setCodingCounter(bufHasTransfered, bufHasCodedBuf);
            decodingTask.setBindCoreidx(bindCoreIdx);
            if (networkRound > 1) {
                es.submit(decodingTask);
            }
            codingSubmitEnd = System.nanoTime();

            readSubmitStart = System.nanoTime();
            for (int i = 0; i < networkRound; i++) {
                transferBufferQueue.clear();
                for (CrailBuffer buf : dataBufferQueue) {
                    buf.limit(networkSizeArray.get(i));
                    if (i == 0) {
                        buf.position(0);
                    } else {
                        buf.position(networkSizeArray.get(i - 1));
                    }
                    transferBufferQueue.add(buf);
                }
                ((CoreErasureCodingInputStream) directStream).readSplitsAsync(transferBufferQueue);
            }
            readSubmitEnd = System.nanoTime();

            transferStart = System.nanoTime();
            for (int i = 0; i < networkRound; i++) {
                subTransferStart[i] = System.nanoTime();
                double ret = ((CoreErasureCodingInputStream) directStream).syncResults(NumDataBlock);
                subTransferEnd[i] = System.nanoTime();

                subUpdateStart[i] = System.nanoTime();
                int j = 0;
                if (networkRound == 1) {
                    codingLib.NativeDecoding(src, eraseData, srcIndices, degradeIndices, NumDataBlock, NumParityBlock, (int) size, degradeNum, bindCoreIdx);

                    for (CrailBuffer buf : eraseBufferQueue) {
                        futureQueue.add(recoverOutputStreams.get(j++).write(buf));
                    }
                } else {
                    cursor_network = networkSizeArray.get(i) / Monec_buffer_split_size;
                    setCounter(bufHasTransfered, cursor_network);
                    hasRecovered = hasCoded;
                    hasCoded = getCounter(bufHasCodedBuf);

                    // hasCoded may be error read (> cursor_network)
                    // solve: retry read until hasCoded <= cursor_network
                    // use optimistic lock to avoid read the modifying hasCoded
                    blockingStart = System.nanoTime();
                    while(hasCoded > cursor_network || hasCoded * Monec_buffer_split_size < hasRecovered) {
                        // blocking until read the collect hasCoded
                        hasCoded = getCounter(bufHasCodedBuf);

                        blockingEnd = System.nanoTime();
                        if(blockingEnd - blockingStart > blockingBound) {
                            System.out.println("while loop recoveryMicroECConcurrentTrans getCounter(bufHasCodedBuf) " + getCounter(bufHasCodedBuf) + " cursor_network " + cursor_network + " hasRecovered " + hasRecovered + " hasCoded " + hasCoded);
                            throw new Exception("recoveryMicroECConcurrentTrans hasCoded error! getCounter(bufHasCodedBuf) " + getCounter(bufHasCodedBuf) + " cursor_network " + cursor_network + " hasRecovered " + hasRecovered + " hasCoded " + hasCoded);
                        }
                    }

                    subHasCoded[i] = hasCoded;
//                    System.out.println("recovery cursor_network " + cursor_network + " hasRecovered " + hasRecovered + " hasCoded "+hasCoded);

                    hasCoded *= Monec_buffer_split_size;
                    if (hasRecovered != hasCoded) {
//                        System.out.println("recovery write buf range " + hasRecovered + " " + hasCoded);

                        for (CrailBuffer buf : eraseBufferQueue) {
                            buf.limit(hasCoded);
                            buf.position(hasRecovered);
                            CrailBuffer tmp = buf.slice();
                            futureQueue.add(recoverOutputStreams.get(j++).write(tmp));
                        }
                    }
                }
                subUpdateEnd[i] = System.nanoTime();
            }
            transferEnd = System.nanoTime();

            syncCodingStart = System.nanoTime();
            // check decoding is whether finished
            while (networkRound != 1 && !(cursor.get(monECSubStripeNum - 1) == '1')) {
                // blocking until decoding finishing
            }
            syncCodingEnd = System.nanoTime();
            coreCache.releaseCore();

            syncTransferStart = System.nanoTime();
            if (networkRound != 1) {
                int j = 0;
                for (CrailBuffer buf : eraseBufferQueue) {
                    buf.limit(Microec_buffer_size);
                    buf.position(hasCoded);
                    CrailBuffer tmp = buf.slice();
                    futureQueue.add(recoverOutputStreams.get(j++).write(tmp));
                }
            }
//            System.out.println("final range " + hasCoded + " " + Microec_buffer_size);

            while (!futureQueue.isEmpty()) {
                Future<CrailResult> future = futureQueue.poll();
                future.get();
            }
            syncTransferEnd = System.nanoTime();
            loopEnd = System.nanoTime();

            everyLoopLatency[ops] = loopEnd - loopStart;
            loopLatencyRecord.get(ops).add(loopEnd - loopStart);
            loopLatencyRecord.get(ops).add((long) bindCoreIdx);
            loopLatencyRecord.get(ops).add(codingSubmitEnd - codingSubmitStart);
            loopLatencyRecord.get(ops).add(readSubmitEnd - readSubmitStart);
            loopLatencyRecord.get(ops).add(transferEnd - transferStart);
            for (int i = 0; i < networkRound; i++) {
                loopLatencyRecord.get(ops).add(subTransferEnd[i] - subTransferStart[i]);
                loopLatencyRecord.get(ops).add(subUpdateEnd[i] - subUpdateStart[i]);
                loopLatencyRecord.get(ops).add((long) subHasCoded[i]);
            }
            loopLatencyRecord.get(ops).add(syncCodingEnd - syncCodingStart);
            loopLatencyRecord.get(ops).add(syncTransferEnd - syncTransferStart);
            ops = ops + 1;
        }

        ((CoreErasureCodingInputStream) directStream).closeSlavesInputStream();
        for (int i = 0; i < degradeNum; i++) {
            recoverOutputStreams.get(i).close();
        }
        es.shutdown();

        System.out.println("ops " + ops);
        System.out.println("final loop (us) " + ((double) (loopEnd - loopStart)) / 1000.0);
        getAvgLoopLatency();

        for (int i = 0; i < loopLatencyRecord.size(); i++) {
            System.out.println("Loop " + i + " " + loopLatencyRecord.get(i));
        }
        getRedundantFileCapacity(recoverFiles);

        fs.getStatistics().print("close");
    }

    void readSequential(String filename, int size, int loop, boolean buffered) throws Exception {
        System.out.println("readSequential, filename " + filename + ", size " + size + ", loop " + loop + ", buffered " + buffered);

        CrailBuffer buf = null;
        if (size == CrailConstants.BUFFER_SIZE) {
            buf = fs.allocateBuffer();
        } else if (size < CrailConstants.BUFFER_SIZE) {
            CrailBuffer _buf = fs.allocateBuffer();
            _buf.clear().limit(size);
            buf = _buf.slice();
        } else {
            buf = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(size));
        }

        //warmup
        ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        bufferQueue.add(buf);
        warmUp(filename, warmup, bufferQueue);

//        CrailFile file=null;
//        for (int i = 0; i < 20; i++) {
//            long metaStart=System.nanoTime();
//            file = fs.lookup(filename).get().asFile();
//            long metaEnd=System.nanoTime();
//            System.out.println("MetaData time "+(metaEnd-metaStart));
//        }

        CrailFile file = fs.lookup(filename).get().asFile();

        CrailBufferedInputStream bufferedStream = file.getBufferedInputStream(file.getCapacity());
        CrailInputStream directStream = file.getDirectInputStream(file.getCapacity());

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        double sumbytes = 0;
        double ops = 0;
        long start = System.currentTimeMillis();
        while (ops < loop) {
            if (buffered) {
                buf.clear();
                double ret = (double) bufferedStream.read(buf.getByteBuffer());
                if (ret > 0) {
                    sumbytes = sumbytes + ret;
                    ops = ops + 1.0;
                    System.out.println("readSequential-buffered-ret > 0");
                } else {
                    ops = ops + 1.0;
                    if (bufferedStream.position() == 0) {
                        break;
                    } else {
                        bufferedStream.seek(0);
                    }
                    System.out.println("readSequential-buffered-ret <= 0");
                }
            } else {
                buf.clear();
                double ret = (double) directStream.read(buf).get().getLen();
                if (ret > 0) {
                    sumbytes = sumbytes + ret;
                    ops = ops + 1.0;
                    System.out.println("readSequential-no buffered-ret > 0");
                } else {
                    ops = ops + 1.0;
                    if (directStream.position() == 0) {
                        break;
                    } else {
                        directStream.seek(0);
                    }
                    System.out.println("readSequential-buffered-ret <= 0");
                }
            }
        }
        long end = System.currentTimeMillis();
        double executionTime = ((double) (end - start)) / 1000.0;
        double throughput = 0.0;
        double latency = 0.0;
        double sumbits = sumbytes * 8.0;
        if (executionTime > 0) {
            throughput = sumbits / executionTime / 1000.0 / 1000.0;
            latency = 1000000.0 * executionTime / ops;
        }
        bufferedStream.close();
        directStream.close();

        System.out.println("execution time " + executionTime);
        System.out.println("ops " + ops);
        System.out.println("sumbytes " + sumbytes);
        System.out.println("throughput " + throughput);
        System.out.println("latency " + latency);

        fs.getStatistics().print("close");
    }

    void readRandom(String filename, int size, int loop, boolean buffered) throws Exception {
        System.out.println("readRandom, filename " + filename + ", size " + size + ", loop " + loop + ", buffered " + buffered);

        CrailBuffer buf = null;
        if (size == CrailConstants.BUFFER_SIZE) {
            buf = fs.allocateBuffer();
        } else if (size < CrailConstants.BUFFER_SIZE) {
            CrailBuffer _buf = fs.allocateBuffer();
            _buf.clear().limit(size);
            buf = _buf.slice();
        } else {
            buf = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(size));
        }

        //warmup
        ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        bufferQueue.add(buf);
        warmUp(filename, warmup, bufferQueue);

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        CrailFile file = fs.lookup(filename).get().asFile();
        CrailBufferedInputStream bufferedStream = file.getBufferedInputStream(file.getCapacity());
        CrailInputStream directStream = file.getDirectInputStream(file.getCapacity());

        double sumbytes = 0;
        double ops = 0;
        long _range = file.getCapacity() - ((long) buf.capacity());
        _range = _range / size;
        double range = (double) _range;
        Random random = new Random();

        long start = System.currentTimeMillis();
        while (ops < loop) {
            if (buffered) {
                buf.clear();
                double _offset = range * random.nextDouble();
                long offset = (long) _offset * size;
                bufferedStream.seek(offset);
                double ret = (double) bufferedStream.read(buf.getByteBuffer());
                if (ret > 0) {
                    sumbytes = sumbytes + ret;
                    ops = ops + 1.0;
                } else {
                    break;
                }

            } else {
                buf.clear();
                double _offset = range * random.nextDouble();
                long offset = (long) _offset * size;
                directStream.seek(offset);
                double ret = (double) directStream.read(buf).get().getLen();
                if (ret > 0) {
                    sumbytes = sumbytes + ret;
                    ops = ops + 1.0;
                } else {
                    break;
                }
            }
        }
        long end = System.currentTimeMillis();
        double executionTime = ((double) (end - start)) / 1000.0;
        double throughput = 0.0;
        double latency = 0.0;
        double sumbits = sumbytes * 8.0;
        if (executionTime > 0) {
            throughput = sumbits / executionTime / 1000.0 / 1000.0;
            latency = 1000000.0 * executionTime / ops;
        }
        bufferedStream.close();
        directStream.close();

        System.out.println("execution time " + executionTime);
        System.out.println("ops " + ops);
        System.out.println("sumbytes " + sumbytes);
        System.out.println("throughput " + throughput);
        System.out.println("latency " + latency);

        fs.getStatistics().print("close");
    }

    void readSequentialAsync(String filename, int size, int loop, int batch) throws Exception {
        System.out.println("readSequentialAsync, filename " + filename + ", size " + size + ", loop " + loop + ", batch " + batch);

        ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        for (int i = 0; i < batch; i++) {
            CrailBuffer buf = null;
            if (size == CrailConstants.BUFFER_SIZE) {
                buf = fs.allocateBuffer();
            } else if (size < CrailConstants.BUFFER_SIZE) {
                CrailBuffer _buf = fs.allocateBuffer();
                _buf.clear().limit(size);
                buf = _buf.slice();
            } else {
                buf = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(size));
            }
            bufferQueue.add(buf);
        }

        //warmup
        warmUp(filename, warmup, bufferQueue);

        //benchmark
        System.out.println("starting benchmark...");
        double sumbytes = 0;
        double ops = 0;
        fs.getStatistics().reset();
        CrailFile file = fs.lookup(filename).get().asFile();
        CrailInputStream directStream = file.getDirectInputStream(file.getCapacity());
        HashMap<Integer, CrailBuffer> futureMap = new HashMap<Integer, CrailBuffer>();
        LinkedBlockingQueue<Future<CrailResult>> futureQueue = new LinkedBlockingQueue<Future<CrailResult>>();
        long start = System.currentTimeMillis();
        for (int i = 0; i < batch - 1 && ops < loop; i++) {
            CrailBuffer buf = bufferQueue.poll();
            buf.clear();
            Future<CrailResult> future = directStream.read(buf);
            futureQueue.add(future);
            futureMap.put(future.hashCode(), buf);
            ops = ops + 1.0;
        }
        while (ops < loop) {
            CrailBuffer buf = bufferQueue.poll();
            buf.clear();
            Future<CrailResult> future = directStream.read(buf);
            futureQueue.add(future);
            futureMap.put(future.hashCode(), buf);

            future = futureQueue.poll();
            CrailResult result = future.get();
            buf = futureMap.get(future.hashCode());
            bufferQueue.add(buf);

            sumbytes = sumbytes + result.getLen();
            ops = ops + 1.0;
        }
        while (!futureQueue.isEmpty()) {
            Future<CrailResult> future = futureQueue.poll();
            CrailResult result = future.get();
            futureMap.get(future.hashCode());
            sumbytes = sumbytes + result.getLen();
            ops = ops + 1.0;
        }
        long end = System.currentTimeMillis();
        double executionTime = ((double) (end - start)) / 1000.0;
        double throughput = 0.0;
        double latency = 0.0;
        double sumbits = sumbytes * 8.0;
        if (executionTime > 0) {
            throughput = sumbits / executionTime / 1000.0 / 1000.0;
            latency = 1000000.0 * executionTime / ops;
        }
        directStream.close();

        System.out.println("execution time " + executionTime);
        System.out.println("ops " + ops);
        System.out.println("sumbytes " + sumbytes);
        System.out.println("throughput " + throughput);
        System.out.println("latency " + latency);

        fs.getStatistics().print("close");
    }

    void readMultiStream(String filename, int size, int loop, int batch) throws Exception {
        System.out.println("readMultiStream, filename " + filename + ", size " + size + ", loop " + loop + ", batch " + batch);

        //warmup
        ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        for (int i = 0; i < warmup; i++) {
            CrailBuffer buf = fs.allocateBuffer().limit(size).slice();
            bufferQueue.add(buf);
        }
        warmUp(filename, warmup, bufferQueue);
        while (!bufferQueue.isEmpty()) {
            CrailBuffer buf = bufferQueue.poll();
            fs.freeBuffer(buf);
        }

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        CrailBuffer _buf = null;
        if (size == CrailConstants.BUFFER_SIZE) {
            _buf = fs.allocateBuffer();
        } else if (size < CrailConstants.BUFFER_SIZE) {
            CrailBuffer __buf = fs.allocateBuffer();
            __buf.clear().limit(size);
            _buf = __buf.slice();
        } else {
            _buf = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(size));
        }
        ByteBuffer buf = _buf.getByteBuffer();
        for (int i = 0; i < loop; i++) {
            CrailBufferedInputStream multiStream = fs.lookup(filename).get().asMultiFile().getMultiStream(batch);
            double sumbytes = 0;
            long _sumbytes = 0;
            double ops = 0;
            buf.clear();
            long start = System.currentTimeMillis();
            int ret = multiStream.read(buf);
            while (ret >= 0) {
                sumbytes = sumbytes + ret;
                long _ret = (long) ret;
                _sumbytes += _ret;
                ops = ops + 1.0;
                buf.clear();
                ret = multiStream.read(buf);
            }
            long end = System.currentTimeMillis();
            multiStream.close();

            double executionTime = ((double) (end - start)) / 1000.0;
            double throughput = 0.0;
            double latency = 0.0;
            double sumbits = sumbytes * 8.0;
            if (executionTime > 0) {
                throughput = sumbits / executionTime / 1000.0 / 1000.0;
                latency = 1000000.0 * executionTime / ops;
            }

            System.out.println("round " + i + ":");
            System.out.println("bytes read " + _sumbytes);
            System.out.println("execution time " + executionTime);
            System.out.println("ops " + ops);
            System.out.println("throughput " + throughput);
            System.out.println("latency " + latency);
        }

        fs.getStatistics().print("close");
    }

    void createReplicasFile(String filename, int loop) throws Exception, InterruptedException {
        System.out.println("createReplicasFile, filename " + filename + ", loop " + loop);

        //warmup
        ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        CrailBuffer buf = fs.allocateBuffer();
        bufferQueue.add(buf);
        warmUp(filename, warmup, bufferQueue);
        fs.freeBuffer(buf);

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        LinkedBlockingQueue<String> pathQueue = new LinkedBlockingQueue<String>();
//		fs.create(filename, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().syncDir();
//		int filecounter = 0;
//		for (int i = 0; i < loop; i++){
//			String name = "" + filecounter++;
//			String f = filename + "/" + name;
//			pathQueue.add(f);
//		}
        pathQueue.add(filename);

        double ops = 0;
        long start = System.currentTimeMillis();
        while (!pathQueue.isEmpty()) {
            String path = pathQueue.poll();
            fs.createReplicasFile(path, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true, RedundancyType.REPLICAS_TYPE).get().syncDir();
        }
        long end = System.currentTimeMillis();
        double executionTime = ((double) (end - start)) / 1000.0;
        double latency = 0.0;
        if (executionTime > 0) {
            latency = 1000000.0 * executionTime / ops;
        }

        System.out.println("execution time " + executionTime);
        System.out.println("ops " + ops);
        System.out.println("latency " + latency);

        fs.getStatistics().print("close");
    }

    void createFile(String filename, int loop) throws Exception, InterruptedException {
        System.out.println("createFile, filename " + filename + ", loop " + loop);

        //warmup
        ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        CrailBuffer buf = fs.allocateBuffer();
        bufferQueue.add(buf);
        warmUp(filename, warmup, bufferQueue);
        fs.freeBuffer(buf);

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        LinkedBlockingQueue<String> pathQueue = new LinkedBlockingQueue<String>();
        fs.create(filename, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().syncDir();
        int filecounter = 0;
        for (int i = 0; i < loop; i++) {
            String name = "" + filecounter++;
            String f = filename + "/" + name;
            pathQueue.add(f);
        }

        double ops = 0;
        long start = System.currentTimeMillis();
        while (!pathQueue.isEmpty()) {
            String path = pathQueue.poll();
            fs.create(path, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().syncDir();
        }
        long end = System.currentTimeMillis();
        double executionTime = ((double) (end - start)) / 1000.0;
        double latency = 0.0;
        if (executionTime > 0) {
            latency = 1000000.0 * executionTime / ops;
        }

        System.out.println("execution time " + executionTime);
        System.out.println("ops " + ops);
        System.out.println("latency " + latency);

        fs.getStatistics().print("close");
    }

    void createFileAsync(String filename, int loop, int batch) throws Exception, InterruptedException {
        System.out.println("createFileAsync, filename " + filename + ", loop " + loop + ", batch " + batch);

        //warmup
        ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        CrailBuffer buf = fs.allocateBuffer();
        bufferQueue.add(buf);
        warmUp(filename, warmup, bufferQueue);
        fs.freeBuffer(buf);

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        LinkedBlockingQueue<Future<CrailNode>> futureQueue = new LinkedBlockingQueue<Future<CrailNode>>();
        LinkedBlockingQueue<CrailFile> fileQueue = new LinkedBlockingQueue<CrailFile>();
        LinkedBlockingQueue<String> pathQueue = new LinkedBlockingQueue<String>();
        fs.create(filename, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().syncDir();

        for (int i = 0; i < loop; i++) {
            String name = "/" + i;
            String f = filename + name;
            pathQueue.add(f);
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < loop; i += batch) {
            //single operation == loop
            for (int j = 0; j < batch; j++) {
                String path = pathQueue.poll();
                Future<CrailNode> future = fs.create(path, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true);
                futureQueue.add(future);
            }
            for (int j = 0; j < batch; j++) {
                Future<CrailNode> future = futureQueue.poll();
                CrailFile file = future.get().asFile();
                fileQueue.add(file);
            }
            for (int j = 0; j < batch; j++) {
                CrailFile file = fileQueue.poll();
                file.syncDir();
            }
        }
        long end = System.currentTimeMillis();
        double executionTime = ((double) (end - start));
        double latency = executionTime * 1000.0 / ((double) loop);
        System.out.println("execution time [ms] " + executionTime);
        System.out.println("latency [us] " + latency);

        fs.delete(filename, true).get().syncDir();

        fs.getStatistics().print("close");

    }

    void createMultiFile(String filename, int storageClass) throws Exception, InterruptedException {
        System.out.println("createMultiFile, filename " + filename);
        fs.create(filename, CrailNodeType.MULTIFILE, CrailStorageClass.get(storageClass), CrailLocationClass.DEFAULT, true).get().syncDir();
    }

    void getKey(String filename, int size, int loop) throws Exception {
        System.out.println("getKey, path " + filename + ", size " + size + ", loop " + loop);

        CrailBuffer buf = fs.allocateBuffer().clear().limit(size).slice();
        CrailFile file = fs.create(filename, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().asFile();
        file.syncDir();
        CrailOutputStream directOutputStream = file.getDirectOutputStream(0);
        directOutputStream.write(buf).get();
        directOutputStream.close();

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        long start = System.currentTimeMillis();
        for (int i = 0; i < loop; i++) {
            CrailInputStream directInputStream = fs.lookup(filename).get().asFile().getDirectInputStream(0);
            buf.clear();
            directInputStream.read(buf).get();
            directInputStream.close();
        }
        long end = System.currentTimeMillis();
        double executionTime = ((double) (end - start));
        double latency = executionTime * 1000.0 / ((double) loop);
        System.out.println("execution time [ms] " + executionTime);
        System.out.println("latency [us] " + latency);

        fs.getStatistics().print("close");
    }

    void getFile(String filename, int loop) throws Exception, InterruptedException {
        System.out.println("getFile, filename " + filename + ", loop " + loop);

        //warmup
        ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        CrailBuffer buf = fs.allocateBuffer();
        bufferQueue.add(buf);
        warmUp(filename, warmup, bufferQueue);
        fs.freeBuffer(buf);

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        double ops = 0;
        long start = System.currentTimeMillis();
        while (ops < loop) {
            ops = ops + 1.0;
            fs.lookup(filename).get().asFile();
        }
        long end = System.currentTimeMillis();
        double executionTime = ((double) (end - start)) / 1000.0;
        double latency = 0.0;
        if (executionTime > 0) {
            latency = 1000000.0 * executionTime / ops;
        }
        System.out.println("execution time " + executionTime);
        System.out.println("ops " + ops);
        System.out.println("latency " + latency);

        fs.getStatistics().print("close");
        fs.close();
    }

    void getFileAsync(String filename, int loop, int batch) throws Exception, InterruptedException {
        System.out.println("getFileAsync, filename " + filename + ", loop " + loop + ", batch " + batch);

        //warmup
        ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        CrailBuffer buf = fs.allocateBuffer();
        bufferQueue.add(buf);
        warmUp(filename, warmup, bufferQueue);
        fs.freeBuffer(buf);

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        LinkedBlockingQueue<Future<CrailNode>> fileQueue = new LinkedBlockingQueue<Future<CrailNode>>();
        long start = System.currentTimeMillis();
        for (int i = 0; i < loop; i++) {
            //single operation == loop
            for (int j = 0; j < batch; j++) {
                Future<CrailNode> future = fs.lookup(filename);
                fileQueue.add(future);
            }
            for (int j = 0; j < batch; j++) {
                Future<CrailNode> future = fileQueue.poll();
                future.get();
            }
        }
        long end = System.currentTimeMillis();
        double executionTime = ((double) (end - start));
        double latency = executionTime * 1000.0 / ((double) batch);
        System.out.println("execution time [ms] " + executionTime);
        System.out.println("latency [us] " + latency);

        fs.getStatistics().print("close");
    }

    void enumerateDir(String filename, int loop) throws Exception {
        System.out.println("reading enumarate dir, path " + filename);

        //warmup
        ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
        CrailBuffer buf = fs.allocateBuffer();
        bufferQueue.add(buf);
        warmUp(filename, warmup, bufferQueue);
        fs.freeBuffer(buf);

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        long start = System.currentTimeMillis();
        for (int i = 0; i < loop; i++) {
            // single operation == loop
            Iterator<String> iter = fs.lookup(filename).get().asDirectory().listEntries();
            while (iter.hasNext()) {
                iter.next();
            }
        }
        long end = System.currentTimeMillis();
        double executionTime = ((double) (end - start));
        double latency = executionTime * 1000.0 / ((double) loop);
        System.out.println("execution time [ms] " + executionTime);
        System.out.println("latency [us] " + latency);

        fs.getStatistics().print("close");
    }

    void browseDir(String filename) throws Exception {
        System.out.println("reading enumarate dir, path " + filename);

        //benchmark
        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        CrailNode node = fs.lookup(filename).get();
        System.out.println("node type is " + node.getType());

        Iterator<String> iter = node.getType() == CrailNodeType.DIRECTORY ? node.asDirectory().listEntries() : node.asMultiFile().listEntries();
        while (iter.hasNext()) {
            String name = iter.next();
            System.out.println(name);
        }
        fs.getStatistics().print("close");
    }

    void early(String filename) throws Exception {
        ByteBuffer buf = ByteBuffer.allocateDirect(32);
        CrailFile file = fs.create(filename, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).early().asFile();
        CrailBufferedOutputStream stream = file.getBufferedOutputStream(0);
        System.out.println("buffered stream initialized");

        Thread.sleep(1000);
        stream.write(buf);
        System.out.println("buffered stream written");

        Thread.sleep(1000);
        stream.write(buf);
        System.out.println("buffered stream written");

        stream.purge();
        stream.close();

        System.out.println("buffered stream closed");

        fs.getStatistics().print("close");
    }

    void writeInt(String filename, int loop) throws Exception {
        System.out.println("writeInt, filename " + filename + ", loop " + loop);

        //benchmark
        System.out.println("starting benchmark...");
        double ops = 0;
        CrailFile file = fs.create(filename, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().asFile();
        CrailBufferedOutputStream outputStream = file.getBufferedOutputStream(loop * 4);
        int intValue = 0;
        System.out.println("starting write at position " + outputStream.position());
        while (ops < loop) {
            System.out.println("writing position " + outputStream.position() + ", value " + intValue);
            outputStream.writeInt(intValue);
            intValue++;
            ops++;
        }
        outputStream.purge().get();
        outputStream.sync().get();

        fs.getStatistics().print("close");
    }

    void readInt(String filename, int loop) throws Exception {
        System.out.println("seek, filename " + filename + ", loop " + loop);

        //benchmark
        System.out.println("starting benchmark...");
        double ops = 0;
        CrailFile file = fs.lookup(filename).get().asFile();
        CrailBufferedInputStream inputStream = file.getBufferedInputStream(loop * 4);
        System.out.println("starting read at position " + inputStream.position());
        while (ops < loop) {
            System.out.print("reading position " + inputStream.position() + ", expected " + inputStream.position() / 4 + " ");
            int intValue = inputStream.readInt();
            System.out.println(", value " + intValue);
            ops++;
        }
        inputStream.close();

        fs.getStatistics().print("close");
    }

    void seekInt(String filename, int loop) throws Exception {
        System.out.println("seek, filename " + filename + ", loop " + loop);

        //benchmark
        System.out.println("starting benchmark...");
        double ops = 0;
        CrailFile file = fs.lookup(filename).get().asFile();
        Random random = new Random();
        long nbrOfInts = file.getCapacity() / 4;
        CrailBufferedInputStream seekStream = file.getBufferedInputStream(loop * 4);
        System.out.println("starting seek phase, nbrOfInts " + nbrOfInts + ", position " + seekStream.position());
        long falseMatches = 0;
        while (ops < loop) {
            int intIndex = random.nextInt((int) nbrOfInts);
            int pos = intIndex * 4;
            seekStream.seek((long) pos);
            int intValue = seekStream.readInt();
            if (intIndex != intValue) {
                falseMatches++;
                System.out.println("reading, position " + pos + ", expected " + pos / 4 + ", ########## value " + intValue);
            } else {
                System.out.println("reading, position " + pos + ", expected " + pos / 4 + ", value " + intValue);
            }
            ops++;
        }
        seekStream.close();
        long end = System.currentTimeMillis();

        System.out.println("falseMatches " + falseMatches);
        fs.getStatistics().print("close");
    }

    void readMultiStreamInt(String filename, int loop, int batch) throws Exception {
        System.out.println("readMultiStreamInt, filename " + filename + ", loop " + loop + ", batch " + batch);

        System.out.println("starting benchmark...");
        fs.getStatistics().reset();
        CrailBufferedInputStream multiStream = fs.lookup(filename).get().asMultiFile().getMultiStream(batch);
        double ops = 0;
        long falseMatches = 0;
        while (ops < loop) {
            System.out.print("reading position " + multiStream.position() + ", expected " + multiStream.position() / 4 + " ");
            long expected = multiStream.position() / 4;
            int intValue = multiStream.readInt();
            if (expected != intValue) {
                falseMatches++;
            }
            System.out.println(", value " + intValue);
            ops++;
        }
        multiStream.close();

        System.out.println("falseMatches " + falseMatches);

        fs.getStatistics().print("close");
    }

    void printLocationClass() throws Exception {
        System.out.println("locationClass " + fs.getLocationClass());
    }

    void locationMap() throws Exception {
        ConcurrentHashMap<String, String> locationMap = new ConcurrentHashMap<String, String>();
        CrailUtils.parseMap(CrailConstants.LOCATION_MAP, locationMap);

        System.out.println("Parsing locationMap " + CrailConstants.LOCATION_MAP);
        for (String key : locationMap.keySet()) {
            System.out.println("key " + key + ", value " + locationMap.get(key));
        }
    }

    void collectionTest(int size, int loop) throws Exception {
        System.out.println("collectionTest, size " + size + ", loop " + loop);

        RingBuffer<Object> ringBuffer = new RingBuffer<Object>(10);
        ArrayBlockingQueue<Object> arrayQueue = new ArrayBlockingQueue<Object>(10);
        LinkedBlockingQueue<Object> listQueue = new LinkedBlockingQueue<Object>();

        Object obj = new Object();
        long start = System.currentTimeMillis();
        for (int i = 0; i < loop; i++) {
            for (int j = 0; j < size; j++) {
                ringBuffer.add(obj);
                Object tmp = ringBuffer.peek();
                tmp = ringBuffer.poll();
            }
        }
        long end = System.currentTimeMillis();
        double executionTime = ((double) (end - start));
        System.out.println("ringbuffer, execution time [ms] " + executionTime);

        start = System.currentTimeMillis();
        for (int i = 0; i < loop; i++) {
            for (int j = 0; j < size; j++) {
                arrayQueue.add(obj);
                Object tmp = arrayQueue.peek();
                tmp = arrayQueue.poll();
            }
        }
        end = System.currentTimeMillis();
        executionTime = ((double) (end - start));
        System.out.println("arrayQueue, execution time [ms] " + executionTime);

        start = System.currentTimeMillis();
        for (int i = 0; i < loop; i++) {
            for (int j = 0; j < size; j++) {
                listQueue.add(obj);
                Object tmp = listQueue.peek();
                tmp = listQueue.poll();
            }
        }
        end = System.currentTimeMillis();
        executionTime = ((double) (end - start));
        System.out.println("arrayQueue, execution time [ms] " + executionTime);
    }


    // TODO: Use undefined num of parameters to warm data structures and java native api
    public void warmupMicroEC(String filename, int operations, ExecutorService es, CodingCoreCache coreCache, EncodingTask encodingTask) throws Exception {
        Random random = new Random();
        String warmupFilename = filename + random.nextInt();
        System.out.println("warmupCreateRedundantFile, filename " + filename + " operation " + operations);

        if (operations > 0) {
            int NumDataBlock = CrailConstants.ERASURECODING_K;
            int NumParityBlock = CrailConstants.ERASURECODING_M;

            CrailConstants.REPLICATION_FACTOR = NumDataBlock + NumParityBlock - 1;
            System.out.println("CrailConstants.REPLICATION_FACTOR has changed to " + CrailConstants.REPLICATION_FACTOR);

            ByteBuffer[] data = new ByteBuffer[NumDataBlock];
            ByteBuffer[] parity = new ByteBuffer[NumParityBlock];
            ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
            ConcurrentLinkedQueue<CrailBuffer> parityBufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
            allocateDataBuffers(data, encodingTask.Microec_buffer_size, dataBufferQueue);
            allocateParityBuffers(parity, encodingTask.Microec_buffer_size, parityBufferQueue);

            ByteBuffer cursor = ByteBuffer.allocateDirect(encodingTask.NumSubStripe);
            String fullzero = getFullZeroString(encodingTask.NumSubStripe);

            long _capacity = operations * encodingTask.Microec_buffer_size;

            CrailFile file = fs.createReplicasFile(warmupFilename, CrailNodeType.DATAFILE, CrailStorageClass.get(0), CrailLocationClass.get(0), true, RedundancyType.ERASURECODING_TYPE).get().asFile();
            CrailOutputStream directStream = ((ReplicasFile) file).getReplicasDirectOutputStream(_capacity);
//            getRedundantFileLocation(file);

            for (int i = 0; i < operations; i++) {
                clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
                clearBufferOfConcurrentLinkedQueue(parityBufferQueue);
                setByteBuffer(cursor, fullzero);
                ((CoreReplicasOutputStream) directStream).clearLatencyLogger();

                ((CoreReplicasOutputStream) directStream).writeReplicasAsync(dataBufferQueue);
                int bindCoreIdx = coreCache.getFreeCoreIdx();
                if (encodingTask.Microec_buffer_size <= minBlockSize) {
                    encodingTask.codingLib.MicroecEncoding(data, parity, cursor, encodingTask.NumSubStripe, encodingTask.NumDataBlock, encodingTask.NumParityBlock, encodingTask.Microec_buffer_size, bindCoreIdx);
                } else {
                    encodingTask.setEncodingBuffer(data, parity, cursor);
                    encodingTask.setBindCoreidx(bindCoreIdx);

                    es.submit(encodingTask);

                    while (cursor.get(encodingTask.NumSubStripe - 1) != '1') {

                    }
                }
                ((CoreReplicasOutputStream) directStream).writeParitySplitsAsync(parityBufferQueue, NumDataBlock, NumParityBlock);
                ((CoreReplicasOutputStream) directStream).syncResults();
                coreCache.releaseCore();
            }

            directStream.close();
//            getRedundantFileLocation(file);

            fs.deleteRedundantFile(file);
        }

    }


    public void warmupByteBuffer(int operations, ByteBuffer... buffers) {
        for (ByteBuffer buf : buffers) {
            String fullzero = getFullZeroString(buf.capacity());

            for (int i = 0; i < operations; i++) {
                setByteBuffer(buf, fullzero);
            }
        }
    }


    public void warmupConcurrentLinkedQueue(int operations, ConcurrentLinkedQueue<CrailBuffer>... queues) {
        for (int i = 0; i < operations; i++) {
            for (ConcurrentLinkedQueue tmp : queues) {
                clearBufferOfConcurrentLinkedQueue(tmp);
            }
        }
    }


    private void warmupCreateRedundantFile(String filename, int operations) throws Exception {
        Random random = new Random();
        String warmupFilename = filename + random.nextInt();
        System.out.println("warmupCreateRedundantFile, filename " + filename + " operation " + operations);

        if (operations > 0) {
            int NumDataBlock = CrailConstants.ERASURECODING_K;
            int NumParityBlock = CrailConstants.ERASURECODING_M;

            CrailConstants.REPLICATION_FACTOR = NumDataBlock + NumParityBlock - 1;
            System.out.println("CrailConstants.REPLICATION_FACTOR has changed to " + CrailConstants.REPLICATION_FACTOR);

            ByteBuffer[] data = new ByteBuffer[NumDataBlock];
            ByteBuffer[] parity = new ByteBuffer[NumParityBlock];
            ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
            ConcurrentLinkedQueue<CrailBuffer> parityBufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
            allocateDataBuffers(data, CrailConstants.BUFFER_SIZE, dataBufferQueue);
            allocateParityBuffers(parity, CrailConstants.BUFFER_SIZE, parityBufferQueue);

            long _capacity = operations * CrailConstants.BUFFER_SIZE;

            CrailFile file = fs.createReplicasFile(warmupFilename, CrailNodeType.DATAFILE, CrailStorageClass.get(0), CrailLocationClass.get(0), true, RedundancyType.ERASURECODING_TYPE).get().asFile();
            CrailOutputStream directStream = ((ReplicasFile) file).getReplicasDirectOutputStream(_capacity);
//            getRedundantFileLocation(file);

            for (int i = 0; i < operations; i++) {
                clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
                clearBufferOfConcurrentLinkedQueue(parityBufferQueue);
                ((CoreReplicasOutputStream) directStream).clearLatencyLogger();

                ((CoreReplicasOutputStream) directStream).writeReplicasAsync(dataBufferQueue);
                ((CoreReplicasOutputStream) directStream).writeParitySplitsAsync(parityBufferQueue, NumDataBlock, NumParityBlock);

                ((CoreReplicasOutputStream) directStream).syncResults();

//                System.out.println("warmupCreateRedundantFile loop "+i+" stream position "+directStream.position()+" stream syncedCapacity "+((CoreReplicasOutputStream) directStream).getSyncedCapacity());
//
//                ((CoreReplicasOutputStream) directStream).seek(0);
//
//                System.out.println("warmupCreateRedundantFile loop "+i+" stream position "+directStream.position()+" stream syncedCapacity "+((CoreReplicasOutputStream) directStream).getSyncedCapacity());
            }

            directStream.close();
//            getRedundantFileLocation(file);

            fs.deleteRedundantFile(file);
        }
    }

    private void warmupCreateRedundantFile(CrailOutputStream directStream, int operations, int bufferSize) throws Exception {
        if (operations > 0) {
            int NumDataBlock = CrailConstants.ERASURECODING_K;
            int NumParityBlock = CrailConstants.ERASURECODING_M;

            CrailConstants.REPLICATION_FACTOR = NumDataBlock + NumParityBlock - 1;
            System.out.println("CrailConstants.REPLICATION_FACTOR has changed to " + CrailConstants.REPLICATION_FACTOR);

            ByteBuffer[] data = new ByteBuffer[NumDataBlock];
            ByteBuffer[] parity = new ByteBuffer[NumParityBlock];
            ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
            ConcurrentLinkedQueue<CrailBuffer> parityBufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
            allocateDataBuffers(data, bufferSize, dataBufferQueue);
            allocateParityBuffers(parity, bufferSize, parityBufferQueue);

//            getRedundantFileLocation(file);

            for (int i = 0; i < operations; i++) {
                clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
                clearBufferOfConcurrentLinkedQueue(parityBufferQueue);
                ((CoreReplicasOutputStream) directStream).clearLatencyLogger();

                ((CoreReplicasOutputStream) directStream).writeReplicasAsync(dataBufferQueue);
                ((CoreReplicasOutputStream) directStream).writeParitySplitsAsync(parityBufferQueue, NumDataBlock, NumParityBlock);

                ((CoreReplicasOutputStream) directStream).syncResults();

                ((CoreReplicasOutputStream) directStream).seek(0);
            }

//            getRedundantFileLocation(file);

        }
    }

    private void warmupCreateRedundantFile(CrailOutputStream directStream, int operations, ByteBuffer[] data, ByteBuffer[] parity, ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue, ConcurrentLinkedQueue<CrailBuffer> parityBufferQueue) throws Exception {
        if (operations > 0) {
            int NumDataBlock = CrailConstants.ERASURECODING_K;
            int NumParityBlock = CrailConstants.ERASURECODING_M;

            CrailConstants.REPLICATION_FACTOR = NumDataBlock + NumParityBlock - 1;
            System.out.println("CrailConstants.REPLICATION_FACTOR has changed to " + CrailConstants.REPLICATION_FACTOR);

//            getRedundantFileLocation(file);

            for (int i = 0; i < operations; i++) {
                clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
                clearBufferOfConcurrentLinkedQueue(parityBufferQueue);
                ((CoreReplicasOutputStream) directStream).clearLatencyLogger();

                ((CoreReplicasOutputStream) directStream).writeReplicasAsync(dataBufferQueue);
                ((CoreReplicasOutputStream) directStream).writeParitySplitsAsync(parityBufferQueue, NumDataBlock, NumParityBlock);

                ((CoreReplicasOutputStream) directStream).syncResults();

                ((CoreReplicasOutputStream) directStream).seek(0);
            }

//            getRedundantFileLocation(file);

        }
    }

    private void warmupMicroEC(CrailOutputStream directStream, int operations, ExecutorService es, CodingCoreCache coreCache, EncodingTask encodingTask, ByteBuffer[] parity, ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue, ConcurrentLinkedQueue<CrailBuffer> parityBufferQueue, ConcurrentLinkedQueue<CrailBuffer> transferQueue, ByteBuffer cursor, List<Integer> networkSizeArray) throws Exception {
        if (operations > 0) {
            int NumDataBlock = CrailConstants.ERASURECODING_K;
            int NumParityBlock = CrailConstants.ERASURECODING_M;
            String fullzero = getFullZeroString(encodingTask.NumSubStripe);
            int networkRound = networkSizeArray.size();
            int Microec_buffer_split_size = encodingTask.Microec_buffer_size / encodingTask.NumSubStripe;

            // only allocate data buffer
            ByteBuffer[] data = new ByteBuffer[NumDataBlock];
            allocateDataBuffers(data, encodingTask.Microec_buffer_size, dataBufferQueue);

            CrailConstants.REPLICATION_FACTOR = NumDataBlock + NumParityBlock - 1;
            System.out.println("CrailConstants.REPLICATION_FACTOR has changed to " + CrailConstants.REPLICATION_FACTOR);

//            getRedundantFileLocation(file);

            for (int i = 0; i < operations; i++) {
                clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
                clearBufferOfConcurrentLinkedQueue(parityBufferQueue);
                setByteBuffer(cursor, fullzero);
                ((CoreReplicasOutputStream) directStream).clearLatencyLogger();

                int bindCoreIdx = coreCache.getFreeCoreIdx();
                if (encodingTask.Microec_buffer_size <= minBlockSize) {
                    encodingTask.codingLib.MicroecEncoding(data, parity, cursor, encodingTask.NumSubStripe, encodingTask.NumDataBlock, encodingTask.NumParityBlock, encodingTask.Microec_buffer_size, bindCoreIdx);
                } else {
                    encodingTask.setEncodingBuffer(data, parity, cursor);
                    encodingTask.setBindCoreidx(bindCoreIdx);

                    es.submit(encodingTask);
                }

                ((CoreReplicasOutputStream) directStream).writeReplicasAsync(dataBufferQueue);

                int networkIndex = 1;
                while (networkIndex <= networkRound) {
                    int tmp = networkSizeArray.get(networkIndex - 1) / Microec_buffer_split_size - 1;

                    if (cursor.get(tmp) == '1') {
                        transferQueue.clear();
                        for (CrailBuffer buf : parityBufferQueue) {
                            buf.limit(networkSizeArray.get(networkIndex - 1));
                            if (networkIndex == 1) {
                                buf.position(0);
                            } else {
                                buf.position(networkSizeArray.get(networkIndex - 2));
                            }

                            CrailBuffer sbuf = buf.slice();
                            transferQueue.add(sbuf);
                        }

                        ((CoreReplicasOutputStream) directStream).writeParitySplitsAsync(transferQueue, NumDataBlock, NumParityBlock);
                        networkIndex++;
                    }
                }
                ((CoreReplicasOutputStream) directStream).syncResults();

                coreCache.releaseCore();
                ((CoreReplicasOutputStream) directStream).seek(0);
            }

//            getRedundantFileLocation(file);

            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            clearBufferOfConcurrentLinkedQueue(parityBufferQueue);
            setByteBuffer(cursor, fullzero);
            ((CoreReplicasOutputStream) directStream).clearLatencyLogger();
            dataBufferQueue.clear();
        }
    }

    private void warmupECPipeline(CrailOutputStream directStream, int operations, CodingCoreCache coreCache, Crailcoding codingLib, ByteBuffer[] subData, ByteBuffer[] subParity, ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue, ConcurrentLinkedQueue<CrailBuffer> parityBufferQueue, ConcurrentLinkedQueue<CrailBuffer> transferQueue, int networkRound, int encodingSplitSize, boolean isPureMonEC, int pureMonECSubStripeNum) throws Exception {
        int bindCoreIdx = -1;
        int NumDataBlock = subData.length;
        int NumParityBlock = subParity.length;

        while (operations-- > 0) {
            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            clearBufferOfConcurrentLinkedQueue(parityBufferQueue);
            ((CoreReplicasOutputStream) directStream).seek(0);
            ((CoreReplicasOutputStream) directStream).clearLatencyLogger();

            for (int i = 0; i < networkRound; i++) {
                transferQueue.clear();

                int j = 0;
                for (CrailBuffer buf : dataBufferQueue) {
                    buf.limit((i + 1) * encodingSplitSize);
                    buf.position(i * encodingSplitSize);

                    CrailBuffer sbuf = buf.slice();
                    transferQueue.add(sbuf);
                    subData[j++] = sbuf.getByteBuffer();
                }

                j = 0;
                for (CrailBuffer buf : parityBufferQueue) {
                    buf.limit((i + 1) * encodingSplitSize);
                    buf.position(i * encodingSplitSize);

                    CrailBuffer sbuf = buf.slice();
                    transferQueue.add(sbuf);
                    subParity[j++] = sbuf.getByteBuffer();
                }

                bindCoreIdx = coreCache.randomGetFreeCoreIdx();
                // Encoding
                if (isPureMonEC) {
                    codingLib.PureMicroecEncoding(subData, subParity, pureMonECSubStripeNum, NumDataBlock, NumParityBlock, encodingSplitSize, bindCoreIdx);
                } else {
                    codingLib.NativeEncoding(subData, subParity, NumDataBlock, NumParityBlock, encodingSplitSize, bindCoreIdx);
                }
                coreCache.releaseCore();

                ((CoreReplicasOutputStream) directStream).writeReplicasAsync(transferQueue);
            }

            // sync all writes
            ((CoreReplicasOutputStream) directStream).syncResults();
        }
    }

    private void warmupMicroEC(CrailOutputStream directStream, int operations, ExecutorService es, CodingCoreCache coreCache, EncodingTask encodingTask, ByteBuffer[] data, ByteBuffer[] parity, ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue, ConcurrentLinkedQueue<CrailBuffer> parityBufferQueue, ConcurrentLinkedQueue<CrailBuffer> transferQueue, ByteBuffer cursor, List<Integer> networkSizeArray) throws Exception {
        if (operations > 0) {
            int NumDataBlock = CrailConstants.ERASURECODING_K;
            int NumParityBlock = CrailConstants.ERASURECODING_M;
            String fullzero = getFullZeroString(encodingTask.NumSubStripe);
            int networkRound = networkSizeArray.size();
            int Microec_buffer_split_size = encodingTask.Microec_buffer_size / encodingTask.NumSubStripe;

            CrailConstants.REPLICATION_FACTOR = NumDataBlock + NumParityBlock - 1;
            System.out.println("CrailConstants.REPLICATION_FACTOR has changed to " + CrailConstants.REPLICATION_FACTOR);

//            getRedundantFileLocation(file);

            for (int i = 0; i < operations; i++) {
                clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
                clearBufferOfConcurrentLinkedQueue(parityBufferQueue);
                setByteBuffer(cursor, fullzero);
                ((CoreReplicasOutputStream) directStream).clearLatencyLogger();
                ((CoreReplicasOutputStream) directStream).seek(0);

                int bindCoreIdx = coreCache.randomGetFreeCoreIdx();
                if (encodingTask.Microec_buffer_size <= minBlockSize) {
                    encodingTask.codingLib.MicroecEncoding(data, parity, cursor, encodingTask.NumSubStripe, encodingTask.NumDataBlock, encodingTask.NumParityBlock, encodingTask.Microec_buffer_size, bindCoreIdx);
                } else {
                    encodingTask.setEncodingBuffer(data, parity, cursor);
                    encodingTask.setBindCoreidx(bindCoreIdx);

                    es.submit(encodingTask);
                }

                ((CoreReplicasOutputStream) directStream).writeReplicasAsync(dataBufferQueue);

                int networkIndex = 1;
                while (networkIndex <= networkRound) {
                    int tmp = networkSizeArray.get(networkIndex - 1) / Microec_buffer_split_size - 1;

                    if (cursor.get(tmp) == '1') {
                        transferQueue.clear();
                        for (CrailBuffer buf : parityBufferQueue) {
                            buf.limit(networkSizeArray.get(networkIndex - 1));
                            if (networkIndex == 1) {
                                buf.position(0);
                            } else {
                                buf.position(networkSizeArray.get(networkIndex - 2));
                            }

                            CrailBuffer sbuf = buf.slice();
                            transferQueue.add(sbuf);
                        }

                        ((CoreReplicasOutputStream) directStream).writeParitySplitsAsync(transferQueue, NumDataBlock, NumParityBlock);
                        networkIndex++;
                    }
                }
                ((CoreReplicasOutputStream) directStream).syncResults();

                coreCache.releaseCore();
            }

//            getRedundantFileLocation(file);

            clearBufferOfConcurrentLinkedQueue(dataBufferQueue);
            clearBufferOfConcurrentLinkedQueue(parityBufferQueue);
            setByteBuffer(cursor, fullzero);
            ((CoreReplicasOutputStream) directStream).clearLatencyLogger();
        }
    }

    private void warmUp(String filename, int operations, ConcurrentLinkedQueue<CrailBuffer> bufferList) throws Exception {
        Random random = new Random();
        String warmupFilename = filename + random.nextInt();
        System.out.println("warmUp, warmupFile " + warmupFilename + ", operations " + operations);
        if (operations > 0) {
            CrailFile warmupFile = fs.create(warmupFilename, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().asFile();
            CrailBufferedOutputStream warmupStream = warmupFile.getBufferedOutputStream(0);
            for (int i = 0; i < operations; i++) {
                CrailBuffer buf = bufferList.poll();
                buf.clear();
                warmupStream.write(buf.getByteBuffer());
                bufferList.add(buf);
            }
            warmupStream.purge().get();
            warmupStream.close();
            fs.delete(warmupFilename, false).get().syncDir();
        }
    }


    public String getRandomString(int length) {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(62);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }


    void warmupCoding(int operarions, ExecutorService es, CodingCoreCache coreCache, EncodingTask encodingTask) throws IOException {

        if (operarions > 0) {
            ByteBuffer[] input = new ByteBuffer[encodingTask.NumDataBlock];
            ByteBuffer[] output = new ByteBuffer[encodingTask.NumParityBlock];

            allocateDataBuffers(input, encodingTask.Microec_buffer_size);
            allocateParityBuffers(output, encodingTask.Microec_buffer_size);

            ByteBuffer cursor = ByteBuffer.allocateDirect(encodingTask.NumSubStripe);
            String fullzero = getFullZeroString(encodingTask.NumSubStripe);

            for (int i = 0; i < operarions; i++) {
                clearBufferOfArray(input);
                clearBufferOfArray(output);
                setByteBuffer(cursor, fullzero);

                int bindCoreIdx = coreCache.getFreeCoreIdx();
                if (encodingTask.Microec_buffer_size <= minBlockSize) {
                    encodingTask.codingLib.MicroecEncoding(input, output, cursor, encodingTask.NumSubStripe, encodingTask.NumDataBlock, encodingTask.NumParityBlock, encodingTask.Microec_buffer_size, bindCoreIdx);
                } else {
                    encodingTask.setEncodingBuffer(input, output, cursor);
                    encodingTask.setBindCoreidx(bindCoreIdx);

                    es.submit(encodingTask);

                    while (cursor.get(encodingTask.NumSubStripe - 1) != '1') {

                    }
                }
                coreCache.releaseCore();
            }
        }
    }


    // coding test: same coding data buffer (filling fixed string for each buffer)
    void testAsyncCodingSame(int size, int loop, int monECSubStripeNum) throws IOException {
        System.out.println("testAsyncCodingSame size " + size + " loop " + loop + " monECSubStripeNum " + monECSubStripeNum);

        int NumDataBlock = CrailConstants.ERASURECODING_K, NumParityBlock = CrailConstants.ERASURECODING_M, Microec_buffer_size = size / NumDataBlock;
        int bindCoreIdx = 11;
        Crailcoding codingLib = new Crailcoding(NumDataBlock, NumParityBlock);
        CodingCoreCache coreCache = new CodingCoreCache();
        everyLoopLatency = new long[loop];

        ByteBuffer[] input = new ByteBuffer[NumDataBlock];
        ByteBuffer[] output = new ByteBuffer[NumParityBlock];
        ByteBuffer cursor = ByteBuffer.allocateDirect(monECSubStripeNum);
        cursor.clear();

        StringBuilder sb = new StringBuilder(monECSubStripeNum);
        for (int i = 0; i < monECSubStripeNum; i++) {
            sb.append("0");
        }
        String fullzero = sb.toString();

        for (int i = 0; i < NumDataBlock; i++) {
            input[i] = ByteBuffer.allocateDirect(Microec_buffer_size);
            setByteBuffer(input[i], getRandomString(Microec_buffer_size));
        }
        for (int i = 0; i < NumParityBlock; i++) {
            output[i] = ByteBuffer.allocateDirect(Microec_buffer_size);
            output[i].clear();
        }
        setByteBuffer(cursor, fullzero);
        EncodingTask encodingTask = new EncodingTask(codingLib, monECSubStripeNum, NumDataBlock, NumParityBlock, Microec_buffer_size);

        ExecutorService es = Executors.newSingleThreadExecutor();
        long start = 0, end = 0;
        for (int i = 0; i < loop; i++) {
            setByteBuffer(cursor, fullzero);
            for (int j = 0; j < NumDataBlock; j++) {
                input[j].clear();
            }
            for (int j = 0; j < NumParityBlock; j++) {
                output[j].clear();
            }
            randomDataBuffers(input, Microec_buffer_size);

            start = System.nanoTime();
            bindCoreIdx = coreCache.randomGetFreeCoreIdx();
//            bindCoreIdx = size > maxSmallObjSize ? coreCache.randomGetFreeCoreIdx() : coreCache.getFreeCoreIdx();
            encodingTask.setEncodingBuffer(input, output, cursor);
            encodingTask.setBindCoreidx(bindCoreIdx);

            if (Microec_buffer_size <= minBlockSize) {
                codingLib.MicroecEncoding(input, output, cursor, monECSubStripeNum, NumDataBlock, NumParityBlock, Microec_buffer_size, bindCoreIdx);
            } else {
                es.submit(encodingTask);

                while (cursor.get(monECSubStripeNum - 1) != '1') {

                }
            }

            coreCache.releaseCore();
            end = System.nanoTime();

            everyLoopLatency[i] = end - start;
        }

        es.shutdown();

        System.out.println("final loop (us) " + ((double) (end - start)) / 1000.0);

        getAvgLoopLatency();
    }

    // coding test: same coding data buffer (filling fixed string for each buffer)
    void testNativePureEncoding(int size, int loop, int encodingSplitSize, boolean isPureEncoding, int NumSubStripe) throws Exception {
        System.out.println("testNativePureEncoding size " + size + " loop " + loop + " encodingSplitSize " + encodingSplitSize + " isPureEncoding " + isPureEncoding + " NumSubStripe " + NumSubStripe);

        int NumDataBlock = CrailConstants.ERASURECODING_K, NumParityBlock = CrailConstants.ERASURECODING_M, blockSize = size / NumDataBlock;
        Crailcoding codingLib = new Crailcoding(NumDataBlock, NumParityBlock);
        CodingCoreCache coreCache = new CodingCoreCache();
        everyLoopLatency = new long[loop];

        ByteBuffer[] input = new ByteBuffer[NumDataBlock];
        ByteBuffer[] output = new ByteBuffer[NumParityBlock];
        ByteBuffer[] subInput = new ByteBuffer[NumDataBlock];
        ByteBuffer[] subOutput = new ByteBuffer[NumParityBlock];

        for (int i = 0; i < NumDataBlock; i++) {
            input[i] = ByteBuffer.allocateDirect(blockSize);
            setByteBuffer(input[i], getRandomString(blockSize));
        }
        for (int i = 0; i < NumParityBlock; i++) {
            output[i] = ByteBuffer.allocateDirect(blockSize);
            output[i].clear();
        }

        // verify NumSubStripe for pure coding
        int bindCoreIdx = 11;
        if (blockSize % NumSubStripe != 0) {
            System.out.println("Microec_buffer_size % NumSubStripe != 0");
            return;
        }
        int Microec_split_buffer_size = blockSize / NumSubStripe;
        if (Microec_split_buffer_size < minBlockSize) {
            NumSubStripe = 1;
        }
        if (encodingSplitSize > blockSize) {
            encodingSplitSize = blockSize;
            System.out.println("encodingSplitSize has changed to " + encodingSplitSize);
        }
        if (size % (encodingSplitSize * NumDataBlock) != 0) {
            throw new Exception("Object size must be an integer multiple of (encodingSplitSize * NumDataBlock)!");
        }
        int networkRound = size / (encodingSplitSize * NumDataBlock);

        long start = 0, end = 0;
        for (int i = 0; i < loop; i++) {
            for (int j = 0; j < NumDataBlock; j++) {
                input[j].clear();
            }
            for (int j = 0; j < NumParityBlock; j++) {
                output[j].clear();
            }
            randomDataBuffers(input, blockSize);

            start = System.nanoTime();

            bindCoreIdx = coreCache.randomGetFreeCoreIdx();
            if (isPureEncoding) {
                codingLib.PureMicroecEncoding(input, output, NumSubStripe, NumDataBlock, NumParityBlock, blockSize, bindCoreIdx);
            } else {
                for (int j = 0; j < networkRound; j++) {
                    for (int k = 0; k < input.length; k++) {
                        input[k].limit((j + 1) * encodingSplitSize);
                        input[k].position(j * encodingSplitSize);

                        subInput[k] = input[k].slice();
                    }

                    for (int k = 0; k < output.length; k++) {
                        output[k].limit((j + 1) * encodingSplitSize);
                        output[k].position(j * encodingSplitSize);

                        subOutput[k] = output[k].slice();
                    }

                    codingLib.NativeEncoding(subInput, subOutput, NumDataBlock, NumParityBlock, encodingSplitSize, bindCoreIdx);
                }
            }
            coreCache.releaseCore();

            end = System.nanoTime();

            everyLoopLatency[i] = end - start;
        }

        System.out.println("final loop (us) " + ((double) (end - start)) / 1000.0);

        getAvgLoopLatency();
    }

    public void callShellByExec(String shellString) {
        BufferedReader reader = null;
        try {
            Process process = Runtime.getRuntime().exec(shellString);
            int exitValue = process.waitFor();
            if (0 != exitValue) {
                System.out.println("call shell failed. error code is :" + exitValue);
            }
            reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = null;
            while ((line = reader.readLine()) != null) {
                System.out.println("mac@wxw %  " + line);
            }
        } catch (Throwable e) {
            System.out.println("call shell failed. " + e);
        }
    }

    // test for random ec buffers
    public void testRandomDataBufferLatency(int size, int loop) throws IOException {
        System.out.println("testRandomDataBufferLatency, size " + size + ", loop " + loop);

        everyLoopLatency = new long[loop];
        int NumDataBlock = CrailConstants.ERASURECODING_K;
        // pure ec split size
        int Microec_buffer_size = size / NumDataBlock;
        long start = 0, end = 0;

        ByteBuffer[] input = new ByteBuffer[NumDataBlock];

        ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue = new ConcurrentLinkedQueue<>();

        allocateDataBuffers(input, Microec_buffer_size, dataBufferQueue);

        for (int i = 0; i < loop; i++) {
            start = System.nanoTime();
            randomDataBuffers(input, Microec_buffer_size);
            end = System.nanoTime();
            everyLoopLatency[i] = end - start;
        }

        System.out.println("final loop (us) " + ((double) (end - start)) / 1000.0);

        getAvgLoopLatency();
    }

    public void testInitCodingStack(int loop){
        System.out.println("testInitCodingStack, loop " + loop);

        everyLoopLatency = new long[loop];
        long start = 0, end = 0;
        int NumDataBlock = CrailConstants.ERASURECODING_K;
        int NumParityBlock = CrailConstants.ERASURECODING_M;
        Crailcoding codinglib = new Crailcoding(NumDataBlock, NumParityBlock);

        ByteBuffer encodingMatrix = ByteBuffer.allocateDirect((NumDataBlock + NumParityBlock) * NumDataBlock);;
        ByteBuffer gTbls = ByteBuffer.allocateDirect(NumDataBlock * NumParityBlock * 32);

        for (int i = 0; i < loop; i++) {
            start = System.nanoTime();
            codinglib.InitAuxiliaries(NumDataBlock, NumParityBlock, encodingMatrix, gTbls);
            end = System.nanoTime();
            everyLoopLatency[i] = end - start;
        }
        codinglib.TestInitAuxiliaries(NumDataBlock, NumParityBlock, encodingMatrix, gTbls);

        System.out.println("final loop (us) " + ((double) (end - start)) / 1000.0);

        getAvgLoopLatency();
    }

    public static void main(String[] args) throws Exception {
        String type = "";
        String filename = "/tmp.dat";
        int size = CrailConstants.BUFFER_SIZE;
        int loop = 1;
        int batch = 1;
        int warmup = 500;
        int experiments = 1;
        boolean keepOpen = false;
        int storageClass = 0;
        int locationClass = 0;
        boolean useBuffered = true;
        boolean skipDir = false;
        int pureMonECSubStripeNum = 1;
        boolean isPureMonEC = false;
        int transferSize = CrailConstants.BUFFER_SIZE;

        String benchmarkTypes = "write|writeAsync|readSequential|readRandom|readSequentialAsync|readMultiStream|"
                + "createFile|createFileAsync|createMultiFile|createReplicasFile|getKey|getFile|getFileAsync|enumerateDir|browseDir|"
                + "writeInt|readInt|seekInt|readMultiStreamInt|printLocationclass";
        Option typeOption = Option.builder("t").desc("type of experiment [" + benchmarkTypes + "]").hasArg().build();
        Option fileOption = Option.builder("f").desc("filename").hasArg().build();
        Option sizeOption = Option.builder("s").desc("buffer size [bytes]").hasArg().build();
        Option loopOption = Option.builder("k").desc("loop [1..n]").hasArg().build();
        Option batchOption = Option.builder("b").desc("batch size [1..n]").hasArg().build();
        Option storageOption = Option.builder("c").desc("storageClass for file [1..n]").hasArg().build();
        Option locationOption = Option.builder("p").desc("locationClass for file [1..n]").hasArg().build();
        Option warmupOption = Option.builder("w").desc("number of warmup operations [1..n]").hasArg().build();
        Option experimentOption = Option.builder("e").desc("number of experiments [1..n]").hasArg().build();
        Option openOption = Option.builder("o").desc("whether to keep the file system open [true|false]").hasArg().build();
        Option skipDirOption = Option.builder("d").desc("skip writing the directory record [true|false]").hasArg().build();
        Option bufferedOption = Option.builder("m").desc("use buffer streams [true|false]").hasArg().build();
        Option pureECSubStripeNumOption = Option.builder("a").desc("pureMonEC substripe number [1..n]").hasArg().build();
        Option isPureMonECOption = Option.builder("i").desc("is PureMonEC when pipeline [true|false]").hasArg().build();
        Option transferSizeOption = Option.builder("n").desc("network transfer size [1..n]").hasArg().build();

        Options options = new Options();
        options.addOption(typeOption);
        options.addOption(fileOption);
        options.addOption(sizeOption);
        options.addOption(loopOption);
        options.addOption(batchOption);
        options.addOption(storageOption);
        options.addOption(locationOption);
        options.addOption(warmupOption);
        options.addOption(experimentOption);
        options.addOption(openOption);
        options.addOption(bufferedOption);
        options.addOption(skipDirOption);
        options.addOption(pureECSubStripeNumOption);
        options.addOption(isPureMonECOption);
        options.addOption(transferSizeOption);

        CommandLineParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, Arrays.copyOfRange(args, 0, args.length));
        if (line.hasOption(typeOption.getOpt())) {
            type = line.getOptionValue(typeOption.getOpt());
        }
        if (line.hasOption(fileOption.getOpt())) {
            filename = line.getOptionValue(fileOption.getOpt());
        }
        if (line.hasOption(sizeOption.getOpt())) {
            size = Integer.parseInt(line.getOptionValue(sizeOption.getOpt()));
        }
        if (line.hasOption(loopOption.getOpt())) {
            loop = Integer.parseInt(line.getOptionValue(loopOption.getOpt()));
        }
        if (line.hasOption(batchOption.getOpt())) {
            batch = Integer.parseInt(line.getOptionValue(batchOption.getOpt()));
        }
        if (line.hasOption(storageOption.getOpt())) {
            storageClass = Integer.parseInt(line.getOptionValue(storageOption.getOpt()));
        }
        if (line.hasOption(locationOption.getOpt())) {
            locationClass = Integer.parseInt(line.getOptionValue(locationOption.getOpt()));
        }
        if (line.hasOption(warmupOption.getOpt())) {
            warmup = Integer.parseInt(line.getOptionValue(warmupOption.getOpt()));
        }
        if (line.hasOption(experimentOption.getOpt())) {
            experiments = Integer.parseInt(line.getOptionValue(experimentOption.getOpt()));
        }
        if (line.hasOption(openOption.getOpt())) {
            keepOpen = Boolean.parseBoolean(line.getOptionValue(openOption.getOpt()));
        }
        if (line.hasOption(bufferedOption.getOpt())) {
            useBuffered = Boolean.parseBoolean(line.getOptionValue(bufferedOption.getOpt()));
        }
        if (line.hasOption(skipDirOption.getOpt())) {
            skipDir = Boolean.parseBoolean(line.getOptionValue(skipDirOption.getOpt()));
        }
        if (line.hasOption(pureECSubStripeNumOption.getOpt())) {
            pureMonECSubStripeNum = Integer.parseInt(line.getOptionValue(pureECSubStripeNumOption.getOpt()));
        }
        if (line.hasOption(isPureMonECOption.getOpt())) {
            isPureMonEC = Boolean.parseBoolean(line.getOptionValue(isPureMonECOption.getOpt()));
        }
        if (line.hasOption(transferSizeOption.getOpt())) {
            transferSize = Integer.parseInt(line.getOptionValue(transferSizeOption.getOpt()));
        }

        CrailBenchmark benchmark = new CrailBenchmark(warmup);
        if (type.equals("write")) {
            benchmark.open();
            benchmark.write(filename, size, loop, storageClass, locationClass, skipDir);
            benchmark.close();
        } else if (type.equalsIgnoreCase("writeAsync")) {
            benchmark.open();
            benchmark.writeAsync(filename, size, loop, batch, storageClass, locationClass, skipDir);
            benchmark.close();
        } else if (type.equalsIgnoreCase("writeReplicas")) {
            benchmark.open();
            benchmark.writeReplicas(filename, size, loop, storageClass, locationClass, skipDir);
            benchmark.close();
        } else if (type.equalsIgnoreCase("writeECPipeline")) {
            benchmark.open();
            benchmark.writeECPipeline(filename, size, transferSize, loop, storageClass, locationClass, pureMonECSubStripeNum, skipDir, isPureMonEC);
            benchmark.close();
        } else if (type.equalsIgnoreCase("writeMicroEC_CodingDescent") || type.equalsIgnoreCase("writeMicroEC")) {
            benchmark.open();
            benchmark.writeMicroEC_CodingDescent(filename, size, loop, storageClass, locationClass, pureMonECSubStripeNum, skipDir);
//            benchmark.writeMicroEC_CodingDescent_1loop(filename, size, loop, storageClass, locationClass, pureMonECSubStripeNum, skipDir);
            benchmark.close();
        } else if (type.equalsIgnoreCase("writeHydra")) {
            benchmark.open();
            benchmark.writeHydra(filename, size, loop, storageClass, locationClass, skipDir);
            benchmark.close();
        } else if (type.equalsIgnoreCase("writePipelineHydra")) {
            benchmark.open();
            benchmark.writePipelineHydra(filename, size, transferSize, loop, storageClass, locationClass, pureMonECSubStripeNum, skipDir);
            benchmark.close();
        } else if (type.equalsIgnoreCase("testAsyncCodingSame")) {
            benchmark.open();
            benchmark.testAsyncCodingSame(size, loop, pureMonECSubStripeNum);
            benchmark.close();
        } else if (type.equalsIgnoreCase("testNativePureEncoding")) {
            benchmark.open();
            benchmark.testNativePureEncoding(size, loop, transferSize, isPureMonEC, pureMonECSubStripeNum);
            benchmark.close();
        } else if (type.equalsIgnoreCase("testNetworkLatency")) {
            benchmark.open();
            benchmark.testNetworkLatency(filename, size, loop);
            benchmark.close();
        } else if (type.equalsIgnoreCase("testDataNetworkLatency")) {
            benchmark.open();
            benchmark.testDataNetworkLatency(filename, size, loop);
            benchmark.close();
        } else if (type.equalsIgnoreCase("testParityNetworkLatency")) {
            benchmark.open();
            benchmark.testParityNetworkLatency(filename, size, loop);
            benchmark.close();
        } else if (type.equalsIgnoreCase("warmupCreateRedundantFile")) {
            benchmark.open();
            benchmark.warmupCreateRedundantFile(filename, loop);
            benchmark.close();
        } else if (type.equalsIgnoreCase("testShellCall")) {
            benchmark.open();
            benchmark.callShellByExec("/home/hadoop/testCrail/usage.sh");
            benchmark.close();
        } else if (type.equalsIgnoreCase("testRandomDataBufferLatency")) {
            benchmark.open();
            benchmark.testRandomDataBufferLatency(size, loop);
            benchmark.close();
        } else if (type.equalsIgnoreCase("testInitCodingStack")) {
            benchmark.open();
            benchmark.testInitCodingStack(loop);
            benchmark.close();
        } else if (type.equalsIgnoreCase("readReplicas")) {
            benchmark.open();
            benchmark.readReplicas(filename, loop);
            benchmark.close();
        } else if (type.equalsIgnoreCase("readErasureCoding") || type.equalsIgnoreCase("readMicroEC")) {
            benchmark.open();
            benchmark.readErasureCoding(filename, loop);
            benchmark.close();
        } else if (type.equalsIgnoreCase("degradeReadReplicas")) {
            benchmark.open();
            benchmark.degradeReadReplicas(filename, loop);
            benchmark.close();
        } else if (type.equalsIgnoreCase("recoveryReplicas")) {
            benchmark.open();
            benchmark.recoveryReplicas(filename, loop);
            benchmark.close();
        } else if (type.equalsIgnoreCase("degradeReadErasureCoding")) {
            benchmark.open();
            benchmark.degradeReadErasureCoding(filename, loop);
            benchmark.close();
        } else if (type.equalsIgnoreCase("recoveryErasureCoding")) {
            benchmark.open();
            benchmark.recoveryErasureCoding(filename, loop);
            benchmark.close();
        } else if (type.equalsIgnoreCase("degradedReadMicroEC")) {
            benchmark.open();
            benchmark.degradedReadMicroEC(filename, pureMonECSubStripeNum, transferSize, loop);
            benchmark.close();
        } else if (type.equalsIgnoreCase("recoveryMicroEC")) {
            benchmark.open();
            benchmark.recoveryMicroEC(filename, pureMonECSubStripeNum, transferSize, loop);
            benchmark.close();
        } else if (type.equalsIgnoreCase("degradedReadMicroECConcurrentTrans")) {
            benchmark.open();
            benchmark.degradedReadMicroECConcurrentTrans(filename, pureMonECSubStripeNum, transferSize, loop);
            benchmark.close();
        } else if (type.equalsIgnoreCase("recoveryMicroECConcurrentTrans")) {
            benchmark.open();
            benchmark.recoveryMicroECConcurrentTrans(filename, pureMonECSubStripeNum, transferSize, loop);
            benchmark.close();
        } else if (type.equalsIgnoreCase("readSequential")) {
            if (keepOpen) benchmark.open();
            for (int i = 0; i < experiments; i++) {
                System.out.println("experiment " + i);
                if (!keepOpen) benchmark.open();
                benchmark.readSequential(filename, size, loop, useBuffered);
                if (!keepOpen) benchmark.close();
            }
            if (keepOpen) benchmark.close();
        } else if (type.equals("readRandom")) {
            if (keepOpen) benchmark.open();
            for (int i = 0; i < experiments; i++) {
                System.out.println("experiment " + i);
                if (!keepOpen) benchmark.open();
                benchmark.readRandom(filename, size, loop, useBuffered);
                if (!keepOpen) benchmark.close();
            }
            if (keepOpen) benchmark.close();
        } else if (type.equalsIgnoreCase("readSequentialAsync")) {
            if (keepOpen) benchmark.open();
            for (int i = 0; i < experiments; i++) {
                System.out.println("experiment " + i);
                if (!keepOpen) benchmark.open();
                benchmark.readSequentialAsync(filename, size, loop, batch);
                if (!keepOpen) benchmark.close();
            }
            if (keepOpen) benchmark.close();
        } else if (type.equalsIgnoreCase("readMultiStream")) {
            if (keepOpen) benchmark.open();
            for (int i = 0; i < experiments; i++) {
                System.out.println("experiment " + i);
                if (!keepOpen) benchmark.open();
                benchmark.readMultiStream(filename, size, loop, batch);
                if (!keepOpen) benchmark.close();
            }
            if (keepOpen) benchmark.close();
        } else if (type.equals("createFile")) {
            benchmark.open();
            benchmark.createFile(filename, loop);
            benchmark.close();
        } else if (type.equals("createFileAsync")) {
            benchmark.open();
            benchmark.createFileAsync(filename, loop, batch);
            benchmark.close();
        } else if (type.equalsIgnoreCase("createMultiFile")) {
            benchmark.open();
            benchmark.createMultiFile(filename, storageClass);
            benchmark.close();
        } else if (type.equals("createReplicasFile")) {
            benchmark.open();
            benchmark.createReplicasFile(filename, loop);
            benchmark.close();
        } else if (type.equalsIgnoreCase("getKey")) {
            benchmark.open();
            benchmark.getKey(filename, size, loop);
            benchmark.close();
        } else if (type.equals("getFile")) {
            benchmark.open();
            benchmark.getFile(filename, loop);
            benchmark.close();
        } else if (type.equals("getFileAsync")) {
            benchmark.open();
            benchmark.getFileAsync(filename, loop, batch);
            benchmark.close();
        } else if (type.equalsIgnoreCase("enumerateDir")) {
            benchmark.open();
            benchmark.enumerateDir(filename, batch);
            benchmark.close();
        } else if (type.equalsIgnoreCase("browseDir")) {
            benchmark.open();
            benchmark.browseDir(filename);
            benchmark.close();
        } else if (type.equalsIgnoreCase("early")) {
            benchmark.open();
            benchmark.early(filename);
            benchmark.close();
        } else if (type.equalsIgnoreCase("writeInt")) {
            benchmark.open();
            benchmark.writeInt(filename, loop);
            benchmark.close();
        } else if (type.equalsIgnoreCase("readInt")) {
            benchmark.open();
            benchmark.readInt(filename, loop);
            benchmark.close();
        } else if (type.equalsIgnoreCase("seekInt")) {
            benchmark.open();
            benchmark.seekInt(filename, loop);
            benchmark.close();
        } else if (type.equalsIgnoreCase("readMultiStreamInt")) {
            benchmark.open();
            benchmark.readMultiStreamInt(filename, loop, batch);
            benchmark.close();
        } else if (type.equalsIgnoreCase("printLocationClass")) {
            benchmark.open();
            benchmark.printLocationClass();
            benchmark.close();
        } else if (type.equalsIgnoreCase("collection")) {
            for (int i = 0; i < experiments; i++) {
                benchmark.collectionTest(size, loop);
            }
        } else if (type.equalsIgnoreCase("locationMap")) {
            benchmark.locationMap();
        } else {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("crail iobench", options);
            System.exit(-1);
        }
    }

}

