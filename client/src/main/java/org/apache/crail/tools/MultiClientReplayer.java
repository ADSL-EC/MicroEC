package org.apache.crail.tools;

import org.apache.crail.CrailBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

public class MultiClientReplayer extends TraceReplayer implements Runnable {
    int idx;
    int size;
    int times;
    String prefix;
    String[] randomStrings;

    public MultiClientReplayer(int idx, String scheme, String operation, String prefix, int size, int times) throws Exception {
        super(scheme);
        this.idx = idx;
        this.size = size;
        this.times = times;
        this.prefix = prefix;
        this.maxBlockSize = size;
        this.maxRandomSize = size;
        this.randomStrings = new String[100];

        open();
        initRequest(operation);
        loopLatency = new long[times];
        allocateBuffer();
        // to warmup
        sendRequests();
    }

    void initRequest(String operation) throws Exception {
        if (operation.equals("write")) {
            TraceRequest request = new TraceRequest(prefix + idx + scheme + operation + size + ".dat", "PUT", size, false);
            testList.add(request);
        } else {
            // TODO degradeRead operation
            throw new Exception("initRequest todo degradeRead operation");
        }

        prepareMetadata(testList);
//        generateRandomStrings();
    }

    void generateRandomStrings(){
        for (int i = 0; i < randomStrings.length; i++) {
            if(scheme.equals("replicas")){
                randomStrings[i] = getRandomString(size);
            } else {
                randomStrings[i] = getRandomString(size / numDataBlock);
            }
        }
    }

    void setByteBuffer(ByteBuffer buffer, String cs) {
        buffer.clear();
        buffer.put(cs.getBytes());
        buffer.clear();
    }

    void randomDataBuffers(ByteBuffer[] arr, int bufferSize) throws IOException {
        for (int i = 0; i < arr.length; i++) {
            setByteBuffer(arr[i], getRandomString(bufferSize));
        }
    }

    void randomDataBuffers(ByteBuffer[] arr, Random random) {
        for (int i = 0; i < arr.length; i++) {
            setByteBuffer(arr[i], randomStrings[random.nextInt(randomStrings.length)]);
        }
    }

    void replicasLoopRequestProcessing(TraceRequest request) throws Exception {
        String type = request.requestType;
        String filename = request.fileName;
        int size = request.size;
        boolean isDegradeRead = request.isDegradeRead;
        Random random = new Random();

        if (type.equals("PUT")) {
            for (int i = 0; i < times; i++) {
//                CrailBuffer dataBuf = dataBufferQueue.peek();
//                setCrailBuffer(dataBuf, getRandomString(size));
//                setCrailBuffer(dataBuf, randomStrings[random.nextInt(randomStrings.length)]);

                loopLatency[i] = replicasWrite(filename, size);
            }
        } else if (type.equals("GET")) {
            for (int i = 0; i < times; i++) {
                loopLatency[i] = replicasRead(filename, size, isDegradeRead);
            }
        } else {
            throw new Exception("replicasLoopRequestProcessing error request type " + type);
        }
    }

    void nativeECLoopRequestProcessing(TraceRequest request) throws Exception {
        String type = request.requestType;
        String filename = request.fileName;
        int size = request.size;
        boolean isDegradeRead = request.isDegradeRead;
        Random random = new Random();

        if (type.equals("PUT")) {
            for (int i = 0; i < times; i++) {
//                randomDataBuffers(input, size / numDataBlock);
//                randomDataBuffers(input, random);

                loopLatency[i] = nativeECWrite(filename, size);
            }
        } else if (type.equals("GET")) {
            if (isDegradeRead) {
                for (int i = 0; i < times; i++) {
                    loopLatency[i] = nativeECDegradeRead(filename, size);
                }
            } else {
                for (int i = 0; i < times; i++) {
                    loopLatency[i] = nativeECRead(filename, size);
                }
            }
        } else {
            throw new Exception("nativeECLoopRequestProcessing error request type " + type);
        }
    }

    void hydraLoopRequestProcessing(TraceRequest request) throws Exception {
        String type = request.requestType;
        String filename = request.fileName;
        int size = request.size;
        boolean isDegradeRead = request.isDegradeRead;
        Random random = new Random();

        if (type.equals("PUT")) {
            for (int i = 0; i < times; i++) {
//                randomDataBuffers(input, size / numDataBlock);
//                randomDataBuffers(input, random);

                loopLatency[i] = hydraWrite(filename, size);
            }
        } else if (type.equals("GET")) {
            if (isDegradeRead) {
                for (int i = 0; i < times; i++) {
                    loopLatency[i] = nativeECDegradeRead(filename, size);
                }
            } else {
                for (int i = 0; i < times; i++) {
                    loopLatency[i] = nativeECRead(filename, size);
                }
            }
        } else {
            throw new Exception("hydraLoopRequestProcessing error request type " + type);
        }
    }

    void microECLoopRequestProcessing(TraceRequest request) throws Exception {
        String type = request.requestType;
        String filename = request.fileName;
        int size = request.size;
        boolean isDegradeRead = request.isDegradeRead;
        Random random = new Random();

        if (type.equals("PUT")) {
            for (int i = 0; i < times; i++) {
//                randomDataBuffers(input, size / numDataBlock);
//                randomDataBuffers(input, random);

                loopLatency[i] = microECWrite(filename, size);
            }
        } else if (type.equals("GET")) {
            if (isDegradeRead) {
                for (int i = 0; i < times; i++) {
                    loopLatency[i] = microECDegradeRead(filename, size);
                }
            } else {
                for (int i = 0; i < times; i++) {
                    loopLatency[i] = nativeECRead(filename, size);
                }
            }
        } else {
            throw new Exception("microECLoopRequestProcessing error request type " + type);
        }
    }

    void sendRequests() throws Exception {
        if (testList.size() == 1) {
            TraceRequest request = testList.get(0);
            if (scheme.equals("replicas")) {
                if (warmList.size() != 0) {
                    replicasRequestProcessing(warmList);
                }
                replicasLoopRequestProcessing(request);
            } else if (scheme.equals("nativeec")) {
                if (warmList.size() != 0) {
                    nativeECRequestProcessing(warmList);
                }
                nativeECLoopRequestProcessing(request);
            } else if (scheme.equals("microec")) {
                if (warmList.size() != 0) {
                    microECRequestProcessing(warmList);
                }
                microECLoopRequestProcessing(request);
            } else if (scheme.equals("hydra")) {
                if (warmList.size() != 0) {
                    hydraRequestProcessing(warmList);
                }
                hydraLoopRequestProcessing(request);
            }
        } else {
            throw new Exception("testList.size() != 1");
        }
    }

    // us
    double getAvgLatency() {
        double sum = 0.0;
        for (int i = 0; i < loopLatency.length; i++) {
            sum += loopLatency[i];
        }

        return Double.parseDouble(String.format("%.2f", sum / 1000.0 / times));
    }

    // thgpt based on us
    double getThoughput() {
        double avgLatency = getAvgLatency();

        return Double.parseDouble(String.format("%.2f", (1000000.0 / avgLatency) * (size * 1.0 / (1024 * 1024))));
    }

    @Override
    public void run() {
        try {
//            System.out.println("client " + idx + " running");

            // each client send requests
            sendRequests();

            closeStreams();
            close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
