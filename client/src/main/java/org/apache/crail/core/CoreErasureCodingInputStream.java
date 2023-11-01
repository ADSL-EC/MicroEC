package org.apache.crail.core;

import org.apache.crail.CrailBuffer;
import org.apache.crail.CrailInputStream;
import org.apache.crail.CrailResult;
import org.apache.crail.conf.CrailConstants;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public class CoreErasureCodingInputStream extends CoreInputStream {
    private CrailInputStream[] slavesInputStream;
    private LinkedBlockingQueue<Future<CrailResult>> futureQueue;

    public CoreErasureCodingInputStream(CoreNode file, long streamId, long readHint) throws Exception {
        super(file, streamId, readHint);
        futureQueue = new LinkedBlockingQueue<Future<CrailResult>>();
        slavesInputStream = new CrailInputStream[CrailConstants.ERASURECODING_K];
        // unify main split and slaves splits
        slavesInputStream[0] = this;

        CoreFile[] slavesFile = ((ReplicasFile) file).getSlavesFile();
        for (int i = 1; i < CrailConstants.ERASURECODING_K; i++) {
            slavesInputStream[i] = slavesFile[i - 1].getDirectInputStream(readHint);
        }
    }

    public CoreErasureCodingInputStream(CoreNode file, long streamId, long readHint, int[] degradeIndices) throws Exception {
        super(file, streamId, readHint);

//        for (int i = 0; i < degradeIndices.length; i++) {
//            System.out.println("degradeIndices "+i+": "+degradeIndices[i]);
//        }

        if (degradeIndices.length > CrailConstants.ERASURECODING_M) {
            throw new Exception("degradeIndices.length > CrailConstants.ERASURECODING_M!");
        }

        futureQueue = new LinkedBlockingQueue<Future<CrailResult>>();
        slavesInputStream = new CrailInputStream[CrailConstants.ERASURECODING_K];

        Map<Integer, Integer> degradeMap = new HashMap<>();
        CoreFile[] slavesFile = ((ReplicasFile) file).getSlavesFile();
        for (int di : degradeIndices) {
            degradeMap.put(di, 0);
        }

//        System.out.println("degradeMap "+degradeMap);

        if (!degradeMap.containsKey(0)) {
            slavesInputStream[0] = this;
//            System.out.println("slaveFile 0");

            int j = 1;
            for (int i = 0; i < (CrailConstants.ERASURECODING_K + CrailConstants.ERASURECODING_M - 1); i++) {
                if (!degradeMap.containsKey(i + 1)) {
                    slavesInputStream[j] = slavesFile[i].getDirectInputStream(readHint);
//                    System.out.println("slavesFile "+(i+1));
                    j++;
                }
                if (j == CrailConstants.ERASURECODING_K) {
                    break;
                }
            }
        } else {
            int j = 0;
            for (int i = 0; i < (CrailConstants.ERASURECODING_K + CrailConstants.ERASURECODING_M - 1); i++) {
                if (!degradeMap.containsKey(i + 1)) {
                    slavesInputStream[j] = slavesFile[i].getDirectInputStream(readHint);
//                    System.out.println("slavesFile "+(i+1));
                    j++;
                }
                if (j == CrailConstants.ERASURECODING_K) {
                    break;
                }
            }
        }
    }

    public void readSplitsAsync(ConcurrentLinkedQueue<CrailBuffer> bufferQueue) throws Exception {
        int i = 0;
        for (CrailBuffer dataBuf : bufferQueue) {
            Future<CrailResult> slaveFuture = slavesInputStream[i].read(dataBuf);
            futureQueue.add(slaveFuture);

            i++;
        }

//        System.out.println("inputStream readSplitsAsync futureQueue size "+futureQueue.size());
    }

    public void closeSlavesInputStream() throws Exception {
        for (int i = 0; i < CrailConstants.ERASURECODING_K; i++) {
            slavesInputStream[i].close();
        }
    }

    public double syncResults() throws Exception {
        double sum = 0.0;
        while (!futureQueue.isEmpty()) {
            Future<CrailResult> future = futureQueue.poll();
            sum += future.get().getLen();
        }

        return sum;
    }

    public double syncResults(int num) throws Exception {
        double sum = 0.0;

        if (futureQueue.size() < num) {
            throw new Exception("syncResults Error! futureQueue.size() < num!");
        }

        for (int i = 0; i < num; i++) {
            Future<CrailResult> future = futureQueue.poll();
            sum += future.get().getLen();
        }

//        System.out.println("inputStream syncResults futureQueue size "+futureQueue.size());
        return sum;
    }

    public void streamsInfo() {
        for (int i = 0; i < CrailConstants.ERASURECODING_K; i++) {
            System.out.println("slavesInputStream " + i + " " + slavesInputStream[i].position());
        }
    }

    public void resetStreams() throws Exception {
        for (int i = 0; i < CrailConstants.ERASURECODING_K; i++) {
            slavesInputStream[i].seek(0);
        }
    }
}
