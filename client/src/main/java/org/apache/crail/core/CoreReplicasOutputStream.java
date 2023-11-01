package org.apache.crail.core;

import org.apache.crail.CrailBuffer;
import org.apache.crail.CrailOutputStream;
import org.apache.crail.CrailResult;
import org.apache.crail.conf.CrailConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public class CoreReplicasOutputStream extends CoreOutputStream {

    private CrailOutputStream[] slavesOutputStream;
    private LinkedBlockingQueue<Future<CrailResult>> futureQueue;
//    private HashMap<Integer, CrailBuffer> futureMap;

    private List<Long> latencyLogger;

    public CoreReplicasOutputStream(CoreNode file, long streamId, long writeHint) throws Exception {
        super(file, streamId, writeHint);
        futureQueue = new LinkedBlockingQueue<Future<CrailResult>>();
//        futureMap = new HashMap<Integer, CrailBuffer>();
        latencyLogger = new ArrayList<>();
        slavesOutputStream = new CrailOutputStream[CrailConstants.REPLICATION_FACTOR];

        CoreFile[] slavesFile = ((ReplicasFile) file).getSlavesFile();
        for (int i = 0; i < CrailConstants.REPLICATION_FACTOR; i++) {
            slavesOutputStream[i] = slavesFile[i].getDirectOutputStream(writeHint);
        }
    }

    public void clearLatencyLogger(){
        latencyLogger.clear();
    }

    public List<Long> getLatencyLogger(){
        return latencyLogger;
    }

    public void writeReplicasAsync(ConcurrentLinkedQueue<CrailBuffer> bufferQueue) throws Exception {
        long start, end;

//        System.out.println("CoreReplicasOutputStream=-writeReplicasAsync-bufferQueue size "+bufferQueue.size());
        int i = -1;
        for (CrailBuffer dataBuf : bufferQueue) {
            start=System.nanoTime();

            if (i == -1) {
                Future<CrailResult> mainFuture = super.write(dataBuf);
                futureQueue.add(mainFuture);
//                futureMap.put(mainFuture.hashCode(), dataBuf);
            } else {
                Future<CrailResult> slaveFuture = slavesOutputStream[i].write(dataBuf);
                futureQueue.add(slaveFuture);
//                futureMap.put(slaveFuture.hashCode(), dataBuf);
            }
            i++;

            end=System.nanoTime();
//            latencyLogger.add(end-start);
        }
    }

    public void writeParitySplitsAsync(ConcurrentLinkedQueue<CrailBuffer> bufferQueue, int k, int m) throws Exception {
        long start, end;

        int i = k-1;
        for(CrailBuffer buf:bufferQueue){
            start=System.nanoTime();

            Future<CrailResult> slaveFuture = slavesOutputStream[i].write(buf);
            futureQueue.add(slaveFuture);
            i++;

            end=System.nanoTime();
//            latencyLogger.add(end-start);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();

        for (int i = 0; i < CrailConstants.REPLICATION_FACTOR; i++) {
            slavesOutputStream[i].close();
        }
    }

    @Override
    public void seek(long pos) throws IOException {
        super.seek(pos);

        for (int i = 0; i < CrailConstants.REPLICATION_FACTOR; i++) {
            ((CoreOutputStream) slavesOutputStream[i]).seek(pos);
        }
    }

    public void syncResults() throws Exception {
        while (!futureQueue.isEmpty()) {
            Future<CrailResult> future = futureQueue.poll();
            future.get();
//            future.get(3, TimeUnit.SECONDS);
        }
    }


}
