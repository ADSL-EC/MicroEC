package org.apache.crail.tools;

import org.apache.crail.*;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.core.CoreErasureCodingInputStream;
import org.apache.crail.core.CoreReplicasOutputStream;
import org.apache.crail.core.RedundancyType;
import org.apache.crail.core.ReplicasFile;
import org.apache.crail.memory.OffHeapBuffer;
import org.apache.crail.utils.CodingCoreCache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TraceReplayer {

    private CrailConfiguration conf;
    private CrailStore fs;

    String scheme;

    // max buffer size
    int maxBlockSize;
    int maxRandomSize;
    int maxSmallObjSize;
    int minBlockSize;
    // microec coding size
    int Microec_buffer_split_size;
    int maxSubstripeNum;
    int minBigObjSize;
    int largerBigObjSize;
    int largestBigObjSize;

    List<TraceRequest> testList;
    List<TraceRequest> warmList;
    Map<String, Exception> skipRequests;
    int numDataBlock, numParityBlock;

    // read and write
    ByteBuffer[] input;
    ByteBuffer[] output;
    ConcurrentLinkedQueue<CrailBuffer> dataBufferQueue;
    ConcurrentLinkedQueue<CrailBuffer> parityBufferQueue;
    ConcurrentLinkedQueue<CrailBuffer> transferQueue;

    // for degrade read
    ByteBuffer[] eraseData;
    ConcurrentLinkedQueue<CrailBuffer> eraseBufferQueue;

    Map<String, CrailFile> fileMapper;
    Map<String, CrailInputStream> inputStreamMapper;
    Map<String, CrailOutputStream> outputStreamMapper;
    // degrade get related data structures
    Map<String, CrailInputStream> degradeInputStreamMapper;

    // degrade and recovery test
    private int degradeNum;
    private int[] degradeIndices;
    private int[] srcIndices;

    Crailcoding codingLib;
    CodingCoreCache coreCache;

    List<Integer> networkSizeArray;
    ByteBuffer cursor;
    EncodingTask encodingTask;
    DecodingTask decodingTask;
    ExecutorService es;
    ByteBuffer bufHasTransfered;
    ByteBuffer bufHasCoded;

    // collect latency of each request
    public long[] loopLatency;

    public TraceReplayer(String scheme) throws IOException {
        this.conf = CrailConfiguration.createConfigurationFromFile();
        this.fs = null;

        this.scheme = scheme;

        // max buffer size
        maxBlockSize = 1024 * 1024 * 1024;
        maxRandomSize = 64 * 1024 * 1024;
        maxSmallObjSize = 128 * 1024;
        minBlockSize = 4096;
        Microec_buffer_split_size = 4096;
        // bufHasTransfered support bound: 127*127+127=16256;
        maxSubstripeNum = 10000;
        minBigObjSize = 100 * 1024 * 1024;
        largerBigObjSize = 500 * 1024 * 1024;
        largestBigObjSize = 1024 * 1024 * 1024;

        testList = new ArrayList<>();
        warmList = new ArrayList<>();
        skipRequests = new HashMap<>();

        fileMapper = new HashMap<>();
        inputStreamMapper = new HashMap<>();
        outputStreamMapper = new HashMap<>();
        // degrade get related data structures
        degradeInputStreamMapper = new HashMap<>();

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

        // write data for put in testList
        dataBufferQueue = new ConcurrentLinkedQueue<>();
        parityBufferQueue = new ConcurrentLinkedQueue<>();
        transferQueue = new ConcurrentLinkedQueue<>();

        // for degrade read
        eraseData = new ByteBuffer[degradeNum];
        eraseBufferQueue = new ConcurrentLinkedQueue<>();

        // for coding
        codingLib = null;
        coreCache = null;
        networkSizeArray = null;
        cursor = null;
        encodingTask = null;
        decodingTask = null;
        es = null;
        bufHasTransfered = null;
        bufHasCoded = null;
    }

    void zeroByteBuffer(ByteBuffer buf) {
        for (int i = 0; i < buf.capacity(); i++) {
            buf.put(i, (byte) 0);
        }
        buf.clear();
    }

    void zeroByteBuffer(ByteBuffer buf, int len) {
        for (int i = 0; i < len; i++) {
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

    void allocateDataBuffers(ByteBuffer[] arr, int bufferSize, ConcurrentLinkedQueue<CrailBuffer> queue) throws IOException {
        for (int i = 0; i < arr.length; i++) {
            arr[i] = ByteBuffer.allocateDirect(bufferSize);
//            setByteBuffer(arr[i], getRandomString(bufferSize));
            queue.add(OffHeapBuffer.wrap(arr[i]));
        }
    }

    void allocateParityBuffers(ByteBuffer[] arr, int bufferSize, ConcurrentLinkedQueue<CrailBuffer> queue) throws IOException {
        for (int i = 0; i < arr.length; i++) {
            arr[i] = ByteBuffer.allocateDirect(bufferSize);
            arr[i].clear();
            queue.add(OffHeapBuffer.wrap(arr[i]));
        }
    }

    void allocateBuffer() throws IOException {
        if (scheme.equals("replicas")) {
            CrailBuffer data = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(maxBlockSize));
            setCrailBuffer(data, getRandomString(maxRandomSize));
            dataBufferQueue.add(data);
            for (int i = 0; i < CrailConstants.REPLICATION_FACTOR; i++) {
                dataBufferQueue.add(data.slice());
            }
        } else if (scheme.equals("nativeec") || scheme.equals("microec") || scheme.equals("hydra")) {
            input = new ByteBuffer[numDataBlock];
            output = new ByteBuffer[numParityBlock];

            allocateDataBuffers(input, maxBlockSize, dataBufferQueue);
            for (CrailBuffer buf : dataBufferQueue) {
                setCrailBuffer(buf, getRandomString(maxRandomSize));
            }
            allocateParityBuffers(output, maxBlockSize, parityBufferQueue);
            for (int i = 0; i < degradeNum; i++) {
                eraseData[i] = output[i];
            }
            for (CrailBuffer buf : parityBufferQueue) {
                eraseBufferQueue.add(buf);

                if (eraseBufferQueue.size() == degradeNum) {
                    break;
                }
            }

            codingLib = new Crailcoding(numDataBlock, numParityBlock);
            coreCache = new CodingCoreCache();
            srcIndices = getDegradeSrcIndices(degradeIndices);

            cursor = ByteBuffer.allocateDirect(maxSubstripeNum);
            encodingTask = new EncodingTask(codingLib, input, output, cursor, numDataBlock, numParityBlock);
            es = Executors.newSingleThreadExecutor();
            bufHasTransfered = ByteBuffer.allocateDirect(2);
            bufHasCoded = ByteBuffer.allocateDirect(2);
            decodingTask = new DecodingTask(codingLib, input, eraseData, cursor, numDataBlock, numParityBlock, srcIndices, degradeIndices, degradeNum, bufHasTransfered, bufHasCoded);
        }
    }

    public List<TraceRequest> getIBMRequestsInfo(String traceFile, List<TraceRequest> list) {
        String line = "";
        String csvSplitBy = ",";

        try (BufferedReader br = new BufferedReader(new FileReader(traceFile))) {
            while ((line = br.readLine()) != null) {
                // use comma as separator
                String[] request = line.split(csvSplitBy);
                boolean isDegradeRead = false;

                if (!request[1].equals("PUT") && !request[1].equals("GET")) {
                    throw new Exception("getIBMRequestsInfo error request type " + request[1]);
                }

                if (request[3].equals("True")) {
                    isDegradeRead = true;
                }
                list.add(new TraceRequest(request[0], request[1], Integer.parseInt(request[2]), isDegradeRead));
//                System.out.println(request[0] + "," + request[1] + "," + request[2] + "," + isDegradeRead);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return list;
    }

    public List<TraceRequest> getYCSBRequestsInfo(String traceFile, List<TraceRequest> list, String prefix, int size) {
        String line = "";
        String csvSplitBy = " ";

        try (BufferedReader br = new BufferedReader(new FileReader(traceFile))) {
            while ((line = br.readLine()) != null) {
                String[] request = line.split(csvSplitBy);
                String type = null;
                boolean isDegradeRead = false;

                if (request[0].equals("I")) {
                    type = "PUT";
                } else if (request[0].equals("R")) {
                    type = "GET";
                } else {
                    throw new Exception("getYCSBRequestsInfo error request type " + request[0]);
                }

                if (request[2].equals("True")) {
                    isDegradeRead = true;
                }
                list.add(new TraceRequest(prefix + request[1], type, size, isDegradeRead));
//                System.out.println(request[1] + "," + type + "," + size + "," + isDegradeRead);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return list;
    }

    void setCrailBuffer(CrailBuffer buffer, String cs) {
        buffer.clear();
        buffer.put(cs.getBytes());
        buffer.clear();
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

    void clearAndSetNewLimit(ConcurrentLinkedQueue<CrailBuffer> bufferQueue, int newLimit) {
        for (CrailBuffer buf : bufferQueue) {
            buf.clear();
            buf.limit(newLimit);
        }
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

    int getDegradeTransferSize(int size) {
        if (size <= 64 * 1024) {
            return size / numDataBlock;
        } else if (size <= 128 * 1024) {
            return 16 * 1024;
        } else if (size <= 512 * 1024) {
            return 32 * 1024;
        } else if (size <= 2 * 1024 * 1024) {
            return 64 * 1024;
        } else if (size <= 8 * 1024 * 1024) {
            return 128 * 1024;
        } else if (size <= 32 * 1024 * 1024) {
            return 256 * 1024;
        } else if (size <= 128 * 1024 * 1024) {
            return 512 * 1024;
        } else if (size <= 512 * 1024 * 1024) {
            return 1024 * 1024;
        } else {
            return 2 * 1024 * 1024;
        }
    }

    public void getExceptionInfo(Map<String, Exception> m) {
        if (m.size() != 0) {
            System.out.println("Got " + m.size() + " Exception!");
            for (String filename : m.keySet()) {
                System.out.println("objectName " + filename + ": ");
                m.get(filename).printStackTrace();
            }
        }
    }

    void prepareMetadata(List<TraceRequest> requestList) throws Exception {
        String type = null;
        String filename = null;
        int size = 0;
        RedundancyType redundancyType = null;
        if (scheme.equals("replicas")) {
            redundancyType = RedundancyType.REPLICAS_TYPE;
        } else if (scheme.equals("nativeec") || scheme.equals("microec") || scheme.equals("hydra")) {
            numDataBlock = CrailConstants.ERASURECODING_K;
            numParityBlock = CrailConstants.ERASURECODING_M;
            redundancyType = RedundancyType.ERASURECODING_TYPE;
            CrailConstants.REPLICATION_FACTOR = CrailConstants.ERASURECODING_K + CrailConstants.ERASURECODING_M - 1;
        } else {
            throw new Exception("prepareMetadata error scheme type " + scheme);
        }
        skipRequests.clear();

        for (TraceRequest request : requestList) {
            type = request.requestType;
            filename = request.fileName;
            if (scheme.equals("replicas")) {
                size = request.size;
            } else {
                size = request.size / numDataBlock;
            }

            if (type.equals("PUT")) {
                try {
                    CrailFile file = fs.createReplicasFile(filename, CrailNodeType.DATAFILE, CrailStorageClass.get(0), CrailLocationClass.get(0), true, redundancyType).get().asFile();
                    CrailOutputStream outputStream = ((ReplicasFile) file).getReplicasDirectOutputStream(size);
                    CrailInputStream inputStream = null;
                    CrailInputStream degradeInputStream = null;
                    if (scheme.equals("replicas")) {
                        inputStream = file.getDirectInputStream(size);
                        degradeInputStream = ((ReplicasFile) file).getDegradeReplicasDirectInputStream(size, degradeIndices);
                    } else {
                        inputStream = ((ReplicasFile) file).getErasureCodingDirectInputStream(size);
                        degradeInputStream = ((ReplicasFile) file).getDegradeErasureCodingDirectInputStream(size, degradeIndices);
                    }

                    fileMapper.put(filename, file);
                    outputStreamMapper.put(filename, outputStream);
                    inputStreamMapper.put(filename, inputStream);
                    degradeInputStreamMapper.put(filename, degradeInputStream);
                } catch (Exception e) {
                    skipRequests.put(filename, e);
                }
            }
        }

        getExceptionInfo(skipRequests);
    }

    // print redundant file location
    void getRedundantFileCapacity(CrailFile file) {
        CrailFile[] slaveFiles = ((ReplicasFile) file).getSlavesFile();
        System.out.println("replicas primary file " + file.getPath() + " capacity " + file.getCapacity());
        for (int i = 0; i < slaveFiles.length; i++) {
            System.out.println("replicas slave file " + slaveFiles[i].getPath() + " capacity " + slaveFiles[i].getCapacity());
        }
    }

    long replicasWrite(String fileName, int size) throws Exception {
        long start, end;
        CrailOutputStream directStream = outputStreamMapper.get(fileName);
        ((CoreReplicasOutputStream) directStream).seek(0);
        clearAndSetNewLimit(dataBufferQueue, size);

        start = System.nanoTime();
        ((CoreReplicasOutputStream) directStream).writeReplicasAsync(dataBufferQueue);
        ((CoreReplicasOutputStream) directStream).syncResults();
        end = System.nanoTime();

        return end - start;
    }

    long replicasRead(String fileName, int size, boolean isDegradeRead) throws Exception {
        long start, end;
        CrailBuffer readBuf = dataBufferQueue.peek();
        readBuf.clear();
        // GET request may just get a part of the whole object
        readBuf.limit(size);

        CrailInputStream directStream = null;
        if (isDegradeRead) {
            directStream = degradeInputStreamMapper.get(fileName);
        } else {
            directStream = inputStreamMapper.get(fileName);
        }
        directStream.seek(0);

        start = System.nanoTime();
        directStream.read(readBuf).get();
        end = System.nanoTime();
        return end - start;
    }

    long nativeECWrite(String fileName, int size) throws Exception {
        long start, end;
        int blockSize = size / numDataBlock;
        int bindCoreIdx = 11;
        CrailOutputStream directStream = outputStreamMapper.get(fileName);
        ((CoreReplicasOutputStream) directStream).seek(0);
        clearAndSetNewLimit(dataBufferQueue, blockSize);
        clearAndSetNewLimit(parityBufferQueue, blockSize);
        transferQueue.clear();

        start = System.nanoTime();
        for (CrailBuffer buf : dataBufferQueue) {
            transferQueue.add(buf);
        }
        for (CrailBuffer buf : parityBufferQueue) {
            transferQueue.add(buf);
        }
//        bindCoreIdx = size > maxSmallObjSize ? coreCache.getRandomCore() : coreCache.getFreeCoreIdx();
        bindCoreIdx = coreCache.getFreeCoreIdx();
        codingLib.NativeEncoding(input, output, numDataBlock, numParityBlock, blockSize, bindCoreIdx);
        coreCache.releaseCore();
        ((CoreReplicasOutputStream) directStream).writeReplicasAsync(transferQueue);
        ((CoreReplicasOutputStream) directStream).syncResults();
        end = System.nanoTime();

        return end - start;
    }

    long hydraWrite(String fileName, int size) throws Exception {
        long start, end;
        int blockSize = size / numDataBlock;
        int bindCoreIdx = 11;
        CrailOutputStream directStream = outputStreamMapper.get(fileName);
        ((CoreReplicasOutputStream) directStream).seek(0);
        clearAndSetNewLimit(dataBufferQueue, blockSize);
        clearAndSetNewLimit(parityBufferQueue, blockSize);

        start = System.nanoTime();
        ((CoreReplicasOutputStream) directStream).writeReplicasAsync(dataBufferQueue);
        bindCoreIdx = size > maxSmallObjSize ? coreCache.getRandomCore() : coreCache.getFreeCoreIdx();
//        bindCoreIdx = coreCache.getFreeCoreIdx();
        codingLib.NativeEncoding(input, output, numDataBlock, numParityBlock, blockSize, bindCoreIdx);
        coreCache.releaseCore();
        ((CoreReplicasOutputStream) directStream).writeParitySplitsAsync(parityBufferQueue, numDataBlock, numParityBlock);
        ((CoreReplicasOutputStream) directStream).syncResults();
        end = System.nanoTime();

        return end - start;
    }

    long nativeECRead(String fileName, int size) throws Exception {
        long start, end;
        int blockSize = size / numDataBlock;
        CrailInputStream directStream = inputStreamMapper.get(fileName);
        clearAndSetNewLimit(dataBufferQueue, blockSize);
        ((CoreErasureCodingInputStream) directStream).resetStreams();

        start = System.nanoTime();
        ((CoreErasureCodingInputStream) directStream).readSplitsAsync(dataBufferQueue);
        ((CoreErasureCodingInputStream) directStream).syncResults();
        end = System.nanoTime();

        return end - start;
    }

    long nativeECDegradeRead(String fileName, int size) throws Exception {
        long start, end;
        int blockSize = size / numDataBlock;
        int bindCoreIdx = 11;
        CrailInputStream directStream = degradeInputStreamMapper.get(fileName);
        clearAndSetNewLimit(dataBufferQueue, blockSize);
        clearAndSetNewLimit(eraseBufferQueue, blockSize);
        ((CoreErasureCodingInputStream) directStream).resetStreams();

        start = System.nanoTime();
        ((CoreErasureCodingInputStream) directStream).readSplitsAsync(dataBufferQueue);
        ((CoreErasureCodingInputStream) directStream).syncResults();
        clearAndSetNewLimit(dataBufferQueue, blockSize);
//        bindCoreIdx = size > maxSmallObjSize ? coreCache.getRandomCore() : coreCache.getFreeCoreIdx();
        bindCoreIdx = coreCache.getFreeCoreIdx();
        codingLib.NativeDecoding(input, eraseData, srcIndices, degradeIndices, numDataBlock, numParityBlock, blockSize, degradeNum, bindCoreIdx);
        coreCache.releaseCore();
        end = System.nanoTime();

        return end - start;
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

    int getCodingBlockSize(int size) {
        if (size <= minBlockSize * numDataBlock) {
            return size / numDataBlock;
        } else if (size <= minBigObjSize) {
            return 4096;
        } else if (size <= largerBigObjSize) {
            return 4 * 4096;
        } else if (size <= largestBigObjSize) {
            return 32 * 1024;
        } else {
            return 64 * 1024;
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

    List<Integer> getDegradeReadNetworkSize(int blockSize, int fixedSize) {
        List<Integer> networkSizeArray = getFixedNetworkSize(blockSize, fixedSize);

        if (networkSizeArray.size() != 1) {
            int diff = blockSize - networkSizeArray.get(networkSizeArray.size() - 1);
            List<Integer> decent = getNetworkSizeArray(fixedSize + diff);
            networkSizeArray.remove(networkSizeArray.size() - 1);
            int base = networkSizeArray.get(networkSizeArray.size() - 1);
            for (int i = 0; i < decent.size(); i++) {
                decent.set(i, decent.get(i) + base);
            }

            networkSizeArray.addAll(decent);
        }

        return networkSizeArray;
    }

    long microECWrite(String fileName, int size) throws Exception {
//        System.out.println("microECWrite fileName "+fileName+" size "+size+" start");
        long start, end;
        int Microec_buffer_size = size / numDataBlock;
        Microec_buffer_split_size = getCodingBlockSize(size);
        int monECSubStripeNum = Microec_buffer_size / Microec_buffer_split_size;
        // TODO increase the size of sub coding size
        if (monECSubStripeNum > maxSubstripeNum) {
            throw new Exception("monECSubStripeNum > maxSubstripeNum, monECSubStripeNum " + monECSubStripeNum + " maxSubstripeNum " + maxSubstripeNum + " size " + size);
        }

        networkSizeArray = getOptimizedNetworkSizeArray(Microec_buffer_size, numDataBlock);
        int networkRound = networkSizeArray.size();
        int bindCoreIdx = 11;
        CrailOutputStream directStream = outputStreamMapper.get(fileName);
        ((CoreReplicasOutputStream) directStream).seek(0);
        clearAndSetNewLimit(dataBufferQueue, Microec_buffer_size);
        clearAndSetNewLimit(parityBufferQueue, Microec_buffer_size);
        transferQueue.clear();
        zeroByteBuffer(cursor, monECSubStripeNum);

        start = System.nanoTime();
        ((CoreReplicasOutputStream) directStream).writeReplicasAsync(dataBufferQueue);

//        bindCoreIdx = size > maxSmallObjSize ? coreCache.randomGetFreeCoreIdx() : coreCache.getFreeCoreIdx();
        bindCoreIdx = coreCache.getFreeCoreIdx();
        encodingTask.setBindCoreidx(bindCoreIdx);
        encodingTask.setSubstripeAndBufferSize(monECSubStripeNum, Microec_buffer_size);
        if (Microec_buffer_size <= minBlockSize || networkRound == 1) {
            codingLib.MicroecEncoding(input, output, cursor, monECSubStripeNum, numDataBlock, numParityBlock, Microec_buffer_size, bindCoreIdx);
        } else {
            es.submit(encodingTask);
        }

        int networkIndex = 1;
        while (networkIndex <= networkRound) {
            // TODO may cause error indirection for random size
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
                ((CoreReplicasOutputStream) directStream).writeParitySplitsAsync(transferQueue, numDataBlock, numParityBlock);
                networkIndex++;
            }
        }
        coreCache.releaseCore();
        ((CoreReplicasOutputStream) directStream).syncResults();
        end = System.nanoTime();

//        System.out.println("microECWrite fileName "+fileName+" size "+size+" bindCoreIdx "+bindCoreIdx+" Microec_buffer_size "+Microec_buffer_size+" latency "+(end-start));
        return end - start;
    }

    long microECDegradeRead(String fileName, int size) throws Exception {
//        System.out.println("microECDegradeRead fileName "+fileName+" size "+size+" start");
        long start, end;
        int Microec_buffer_size = size / numDataBlock;
        Microec_buffer_split_size = getCodingBlockSize(size);
        int monECSubStripeNum = Microec_buffer_size / Microec_buffer_split_size;
        if (monECSubStripeNum > maxSubstripeNum) {
            throw new Exception("monECSubStripeNum > maxSubstripeNum, monECSubStripeNum " + monECSubStripeNum + " maxSubstripeNum " + maxSubstripeNum + " size " + size);
        }

        int transferSize = getDegradeTransferSize(size);
        networkSizeArray = getDegradeReadNetworkSize(Microec_buffer_size, transferSize);
        int networkRound = networkSizeArray.size();
        int bindCoreIdx = 11;
        int cursor_network = 0, hasCoded = 0;
        CrailInputStream directStream = degradeInputStreamMapper.get(fileName);
        ((CoreErasureCodingInputStream) directStream).resetStreams();
        clearAndSetNewLimit(dataBufferQueue, Microec_buffer_size);
        clearAndSetNewLimit(parityBufferQueue, Microec_buffer_size);
        transferQueue.clear();
        zeroByteBuffer(cursor, monECSubStripeNum);
        zeroByteBuffer(bufHasTransfered);
        zeroByteBuffer(bufHasCoded);

        start = System.nanoTime();
//        bindCoreIdx = size > maxSmallObjSize ? coreCache.randomGetFreeCoreIdx() : coreCache.getFreeCoreIdx();
        bindCoreIdx = coreCache.getFreeCoreIdx();
        decodingTask.setBindCoreidx(bindCoreIdx);
        decodingTask.setSubstripeAndBufferSize(monECSubStripeNum, Microec_buffer_size);
        if (networkRound > 1) {
            es.submit(decodingTask);
        }

        for (int i = 0; i < networkRound; i++) {
            transferQueue.clear();
            for (CrailBuffer buf : dataBufferQueue) {
                buf.limit(networkSizeArray.get(i));
                if (i == 0) {
                    buf.position(0);
                } else {
                    buf.position(networkSizeArray.get(i - 1));
                }
                transferQueue.add(buf);
            }
            ((CoreErasureCodingInputStream) directStream).readSplitsAsync(transferQueue);
            ((CoreErasureCodingInputStream) directStream).syncResults();
            if (networkRound == 1) {
                codingLib.NativeDecoding(input, eraseData, srcIndices, degradeIndices, numDataBlock, numParityBlock, Microec_buffer_size, degradeNum, bindCoreIdx);
            } else {
                cursor_network = networkSizeArray.get(i) / Microec_buffer_split_size;
                setCounter(bufHasTransfered, cursor_network);
//                hasCoded = getCounter(bufHasCoded);
            }
        }
        while (networkRound != 1 && !(cursor.get(monECSubStripeNum - 1) == '1')) {
            // blocking until decoding finishing
        }
        coreCache.releaseCore();
        end = System.nanoTime();

//        System.out.println("microECDegradeRead fileName "+fileName+" size "+size+" end");
        return end - start;
    }

    void replicasRequestProcessing(List<TraceRequest> requestList) throws Exception {
        String type = null;
        String filename = null;
        int size = 0;
        boolean isDegradeRead = false;

        for (int i = 0; i < requestList.size(); i++) {
            TraceRequest request = requestList.get(i);
            type = request.requestType;
            filename = request.fileName;
            size = request.size;
            isDegradeRead = request.isDegradeRead;

            if (type.equals("PUT")) {
                loopLatency[i] = replicasWrite(filename, size);
            } else if (type.equals("GET")) {
                loopLatency[i] = replicasRead(filename, size, isDegradeRead);
            } else {
                throw new Exception("replicasRequestProcessing error request type " + type);
            }
        }
    }

    void nativeECRequestProcessing(List<TraceRequest> requestList) throws Exception {
        String type = null;
        String filename = null;
        int size = 0;
        boolean isDegradeRead = false;

        for (int i = 0; i < requestList.size(); i++) {
            TraceRequest request = requestList.get(i);
            type = request.requestType;
            filename = request.fileName;
            size = request.size;
            isDegradeRead = request.isDegradeRead;

            if (type.equals("PUT")) {
                loopLatency[i] = nativeECWrite(filename, size);
            } else if (type.equals("GET")) {
                if (isDegradeRead) {
                    loopLatency[i] = nativeECDegradeRead(filename, size);
                } else {
                    loopLatency[i] = nativeECRead(filename, size);
                }
            } else {
                throw new Exception("nativeECRequestProcessing error request type " + type);
            }
        }
    }

    void hydraRequestProcessing(List<TraceRequest> requestList) throws Exception {
        String type = null;
        String filename = null;
        int size = 0;
        boolean isDegradeRead = false;

        for (int i = 0; i < requestList.size(); i++) {
            TraceRequest request = requestList.get(i);
            type = request.requestType;
            filename = request.fileName;
            size = request.size;
            isDegradeRead = request.isDegradeRead;

            if (type.equals("PUT")) {
                loopLatency[i] = hydraWrite(filename, size);
            } else if (type.equals("GET")) {
                if (isDegradeRead) {
                    loopLatency[i] = nativeECDegradeRead(filename, size);
                } else {
                    loopLatency[i] = nativeECRead(filename, size);
                }
            } else {
                throw new Exception("hydraRequestProcessing error request type " + type);
            }
        }
    }

    void microECRequestProcessing(List<TraceRequest> requestList) throws Exception {
        String type = null;
        String filename = null;
        int size = 0;
        boolean isDegradeRead = false;

        for (int i = 0; i < requestList.size(); i++) {
            TraceRequest request = requestList.get(i);
            type = request.requestType;
            filename = request.fileName;
            size = request.size;
            isDegradeRead = request.isDegradeRead;

            if (type.equals("PUT")) {
                loopLatency[i] = microECWrite(filename, size);
            } else if (type.equals("GET")) {
                if (isDegradeRead) {
                    loopLatency[i] = microECDegradeRead(filename, size);
                } else {
                    loopLatency[i] = nativeECRead(filename, size);
                }
            } else {
                throw new Exception("microECRequestProcessing error request type " + type);
            }
        }
    }

    public void initIBMTrace(String warmPath, String testPath) throws Exception {
        getIBMRequestsInfo(warmPath, warmList);
        getIBMRequestsInfo(testPath, testList);

        int largerSize = warmList.size() > testList.size() ? warmList.size() : testList.size();
        loopLatency = new long[largerSize];

        prepareMetadata(warmList);
        prepareMetadata(testList);
    }

    public void initYCSBTrace(String warmPath, String testPath, String prefix, int size) throws Exception {
        getYCSBRequestsInfo(warmPath, warmList, prefix, size);
        getYCSBRequestsInfo(testPath, testList, prefix, size);

        int largerSize = warmList.size() > testList.size() ? warmList.size() : testList.size();
        loopLatency = new long[largerSize];
        maxBlockSize = size;
        maxRandomSize = size;

        prepareMetadata(warmList);
        prepareMetadata(testList);
    }

    public void traceTest() throws Exception {
//        System.out.println("traceTest " + " " + scheme);

        // schemeType: replicas, nativeec, microec
        allocateBuffer();
        if (scheme.equals("replicas")) {
            replicasRequestProcessing(warmList);
            replicasRequestProcessing(testList);
        } else if (scheme.equals("nativeec")) {
            nativeECRequestProcessing(warmList);
            nativeECRequestProcessing(testList);
        } else if (scheme.equals("microec")) {
            microECRequestProcessing(warmList);
            microECRequestProcessing(testList);
        } else if (scheme.equals("hydra")) {
            hydraRequestProcessing(warmList);
            hydraRequestProcessing(testList);
        }

        getRequestLatency();
        closeStreams();
    }

    void getRequestLatency() {
        for (int i = 0; i < testList.size(); i++) {
            System.out.println(i + "," + testList.get(i).size + "," + testList.get(i).requestType + "," + testList.get(i).isDegradeRead + "," + loopLatency[i] / 1000.0);
        }
    }

    void closeStreams() throws Exception {
        for (CrailOutputStream os : outputStreamMapper.values()) {
            os.close();
        }
        if (scheme.equals("replicas")) {
            for (CrailInputStream is : degradeInputStreamMapper.values()) {
                is.close();
            }
        } else {
            for (CrailInputStream is : degradeInputStreamMapper.values()) {
                ((CoreErasureCodingInputStream) is).closeSlavesInputStream();
            }

            es.shutdown();
        }
    }

    public void open() throws Exception {
        if (fs == null) {
            this.fs = CrailStore.newInstance(conf);
        }
    }

    public void close() throws Exception {
        if (fs != null) {
            fs.close();
            fs = null;
        }
    }

}
