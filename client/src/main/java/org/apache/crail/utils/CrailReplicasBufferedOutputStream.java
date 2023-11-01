package org.apache.crail.utils;

import org.apache.crail.CrailBufferedOutputStream;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.core.CoreFile;
import org.apache.crail.core.ReplicasFile;

import java.io.IOException;
import java.nio.ByteBuffer;

public class CrailReplicasBufferedOutputStream extends CrailBufferedOutputStream {
    private CrailBufferedOutputStream[] slavesBufferedOutputStream;

    public CrailReplicasBufferedOutputStream(ReplicasFile file, long writeHint) throws Exception {
        super(file, writeHint);

        slavesBufferedOutputStream=new CrailBufferedOutputStream[CrailConstants.REPLICATION_FACTOR];
        CoreFile[] slavesFile = file.getSlavesFile();
        for (int i = 0; i < CrailConstants.REPLICATION_FACTOR; i++) {
            slavesBufferedOutputStream[i]=slavesFile[i].getBufferedOutputStream(writeHint);
        }
    }

    @Override
    public void write(ByteBuffer dataBuf) throws IOException {
//        System.out.println("CrailReplicasBufferedOutputstream write execute!");

        dataBuf.mark();
//        System.out.println("buf info before:" + dataBuf.position() + " " + dataBuf.limit() + " " + dataBuf.capacity());
        long mainReplicaStartTime = System.nanoTime();
        super.write(dataBuf);
        long mainReplicaEndTime = System.nanoTime();
//        System.out.println("buf info after:" + dataBuf.position() + " " + dataBuf.limit() + " " + dataBuf.capacity());
//        System.out.println("Main replica write time:"+(mainReplicaEndTime-mainReplicaStartTime));

        for(CrailBufferedOutputStream bufferedOutputStream : slavesBufferedOutputStream){
            // reset before write
            dataBuf.reset();

//            System.out.println("buf info before:" + dataBuf.position() + " " + dataBuf.limit() + " " + dataBuf.capacity());
            long slaveReplicaStartTime = System.nanoTime();
            bufferedOutputStream.write(dataBuf);
            long slaveReplicaEndTime = System.nanoTime();
//            System.out.println("buf info after:" + dataBuf.position() + " " + dataBuf.limit() + " " + dataBuf.capacity());

//            System.out.println("Slave replica write time:"+(slaveReplicaEndTime-slaveReplicaStartTime));
        }
    }

    @Override
    public void close() throws IOException {
        super.close();

        for(CrailBufferedOutputStream bufferedOutputStream : slavesBufferedOutputStream){
            bufferedOutputStream.close();
        }
    }
}
