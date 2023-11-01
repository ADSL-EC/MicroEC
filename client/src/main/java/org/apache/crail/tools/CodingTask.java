package org.apache.crail.tools;

import java.nio.ByteBuffer;

public abstract class CodingTask {

    ByteBuffer[] data;
    ByteBuffer cursor;
    int bindCoreidx;

    public Crailcoding codingLib;
    public int NumSubStripe;
    public int NumDataBlock;
    public int NumParityBlock;
    public int Microec_buffer_size;

    public void setBindCoreidx(int bindCoreidx) {
        this.bindCoreidx=bindCoreidx;
    }
}
