package org.apache.crail.core;

import org.apache.crail.CrailBlockLocation;
import org.apache.crail.CrailBufferedOutputStream;
import org.apache.crail.CrailInputStream;
import org.apache.crail.CrailOutputStream;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.FileInfo;
import org.apache.crail.utils.CrailReplicasBufferedOutputStream;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReplicasFile extends CoreFile {

    private CoreFile[] slavesFile;

    public CoreFile[] getSlavesFile() {
        return slavesFile;
    }

    public void setSlavesFile(CoreNode[] slavesFile) {
        for(int i=0;i<CrailConstants.REPLICATION_FACTOR;i++){
            if(slavesFile[i] instanceof CoreFile){
                this.slavesFile[i]=(CoreFile) slavesFile[i];
            }
        }
    }

    public ReplicasFile(CoreDataStore fs, FileInfo fileInfo, String path) {
        super(fs, fileInfo, path);

        slavesFile = new CoreFile[CrailConstants.REPLICATION_FACTOR];
    }

    @Override
    public CoreFile asFile() throws Exception {
        return super.asFile();
    }

    @Override
    public CoreNode syncDir() throws Exception {
        for(int i=0;i<CrailConstants.REPLICATION_FACTOR;i++){
            slavesFile[i]=(CoreFile) slavesFile[i].syncDir();
        }

        return super.syncDir();
    }

    public ReplicasFile(CoreNode coreNode) {
        super(coreNode.getFileSystem(), coreNode.getFileInfo(), coreNode.getPath());
        slavesFile = new CoreFile[CrailConstants.REPLICATION_FACTOR];
        this.setSyncOperations(coreNode.getSyncOperations());
    }

//    public CrailOutputStream[] getSlavesOutputStream(long writeHint) throws Exception {
//        CrailOutputStream[] slavesOutputStream=new CrailOutputStream[CrailConstants.REPLICATION_FACTOR];
//        for(int i=0;i<CrailConstants.REPLICATION_FACTOR;i++){
//            slavesOutputStream[i]=slavesFile[i].getDirectOutputStream(writeHint);
//        }
//
//        return slavesOutputStream;
//    }
//
//    void closeSLavesOutputStream(CoreOutputStream[] streams) throws Exception {
//        for(int i=0;i<CrailConstants.REPLICATION_FACTOR;i++){
//            slavesFile[i].closeOutputStream(streams[i]);
//        }
//    }

    public CrailInputStream getDegradeReplicasDirectInputStream(long readHint, int[] degradeIndices) throws Exception {
        for (int i = 0; i < (CrailConstants.REPLICATION_FACTOR + 1); i++) {
            boolean flag=true;
            for (int j = 0; j < degradeIndices.length; j++) {
                if(degradeIndices[j]==i){
                    flag=false;
                    break;
                }
            }
            if(flag){
                if(i==0){
                    return this.getDirectInputStream(readHint);
                } else {
                    return slavesFile[i-1].getDirectInputStream(readHint);
                }
            }
        }

        throw new Exception("getDegradeReplicasDirectInputStream error!");
    }

    @Override
    public CrailBufferedOutputStream getBufferedOutputStream(long writeHint) throws Exception {
        return new CrailReplicasBufferedOutputStream(this,writeHint);
    }


    public List<List<CrailBlockLocation>> getSlavesBlockLocations(long start, long len) throws Exception {
        List<List<CrailBlockLocation>> blockLocations=new ArrayList<>();
        blockLocations.add(Arrays.asList(fs.getBlockLocations(path, start, len)));

        for (int i = 0; i < CrailConstants.REPLICATION_FACTOR; i++) {
            blockLocations.add(Arrays.asList(fs.getBlockLocations(slavesFile[i].getPath(), start, len)));
        }

        return blockLocations;
    }
}
