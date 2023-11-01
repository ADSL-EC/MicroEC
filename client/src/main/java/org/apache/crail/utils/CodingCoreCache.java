package org.apache.crail.utils;

import org.apache.crail.conf.CrailConstants;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class CodingCoreCache {
    private RandomAccessFile mappedFile;
    private FileChannel fileChannel;
    private MappedByteBuffer sharedBuffer;
    private int[] coreIdx;
    private Map<Integer, Integer> idxMapper;
    private int currentUseIdx;
    private Random random;

    public CodingCoreCache() throws IOException {
        String[] configCores = CrailConstants.CODING_CORE.split(",");
        coreIdx = new int[configCores.length];
        idxMapper = new HashMap<>();
        for (int i = 0; i < configCores.length; i++) {
            coreIdx[i] = Integer.parseInt(configCores[i]);
            idxMapper.put(coreIdx[i], i);
        }
        mappedFile = new RandomAccessFile(CrailConstants.CACHE_PATH + "/shared.dat", "rw");
        fileChannel = mappedFile.getChannel();
        sharedBuffer = fileChannel.map(MapMode.READ_WRITE, 0, coreIdx.length);
        currentUseIdx = -1;
        random = new Random();
    }

    public MappedByteBuffer getSharedBuffer() {
        return sharedBuffer;
    }

    public int[] getCoreIdx() {
        return coreIdx;
    }

    public Map<Integer, Integer> getIdxMapper() {
        return idxMapper;
    }

    public FileLock getLock() throws IOException {
        return fileChannel.lock();
    }

    // TODO: current using a blocking way (keep polling) to find a free core (no lock)
    // sequentially get a free CoreIdx
    public int getFreeCoreIdx() {
        int idx = -1;
        while (idx == -1) {
            for (int i = 0; i < coreIdx.length; i++) {
                if (sharedBuffer.get(idxMapper.get(coreIdx[i])) == 0) {
                    // get core
                    idx = coreIdx[i];
                    // set the flag
                    sharedBuffer.put(idxMapper.get(coreIdx[i]), (byte) 1);
                    break;
                }
            }
        }

        this.currentUseIdx = idx;
        return idx;
    }

    // randomly get a free CoreIdx with FIFO blocking
    public int randomGetFreeCoreIdx() {
        int idx = -1;
        while (idx == -1) {
            int acquireIdx=random.nextInt(coreIdx.length);
            if (sharedBuffer.get(idxMapper.get(coreIdx[acquireIdx]))==0) {
                // get core
                idx=coreIdx[acquireIdx];
                // set the flag
                sharedBuffer.put(idxMapper.get(coreIdx[acquireIdx]), (byte) 1);
                break;
            }
        }

        this.currentUseIdx = idx;
        return idx;
    }

    // randomly get a core without considering FIFO blocking
    public int getRandomCore() {
        return random.nextInt(CrailConstants.CODING_NUMCORE);
    }

    public int getCurrentUseIdx() {
        if (currentUseIdx == -1) {
            return getFreeCoreIdx();
        } else {
            return currentUseIdx;
        }
    }

    public void releaseCore() {
        if (currentUseIdx != -1) {
            sharedBuffer.put(idxMapper.get(currentUseIdx), (byte) 0);
        }
    }

    public void releaseCore(int idx) {
        if (idxMapper.containsKey(idx)) {
            sharedBuffer.put(idxMapper.get(idx), (byte) 0);
        }
    }
}
