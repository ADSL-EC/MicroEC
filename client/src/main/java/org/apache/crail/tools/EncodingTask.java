package org.apache.crail.tools;

import java.nio.ByteBuffer;

public class EncodingTask extends CodingTask implements Runnable {

    ByteBuffer[] parity;

    public EncodingTask(Crailcoding codingLib, ByteBuffer[] data, ByteBuffer[] parity, ByteBuffer cursor, int NumSubStripe, int NumDataBlock, int NumParityBlock, int Microec_buffer_size, int bindCoreidx) {
        this.codingLib = codingLib;
        this.data = data;
        this.parity = parity;
        this.cursor = cursor;
        this.NumSubStripe = NumSubStripe;
        this.NumDataBlock = NumDataBlock;
        this.NumParityBlock = NumParityBlock;
        this.Microec_buffer_size = Microec_buffer_size;
        this.bindCoreidx = bindCoreidx;
    }

    public EncodingTask(Crailcoding codingLib, int NumSubStripe, int NumDataBlock, int NumParityBlock, int Microec_buffer_size, int bindCoreidx) {
        this.codingLib = codingLib;
        this.data = null;
        this.parity = null;
        this.cursor = null;
        this.NumSubStripe = NumSubStripe;
        this.NumDataBlock = NumDataBlock;
        this.NumParityBlock = NumParityBlock;
        this.Microec_buffer_size = Microec_buffer_size;
        this.bindCoreidx = bindCoreidx;
    }

    public EncodingTask(Crailcoding codingLib, int NumSubStripe, int NumDataBlock, int NumParityBlock, int Microec_buffer_size) {
        this.codingLib = codingLib;
        this.data = null;
        this.parity = null;
        this.cursor = null;
        this.NumSubStripe = NumSubStripe;
        this.NumDataBlock = NumDataBlock;
        this.NumParityBlock = NumParityBlock;
        this.Microec_buffer_size = Microec_buffer_size;
        this.bindCoreidx = 11;
    }

    public EncodingTask(Crailcoding codingLib, ByteBuffer[] data, ByteBuffer[] parity, ByteBuffer cursor, int NumDataBlock, int NumParityBlock) {
        this.codingLib = codingLib;
        this.data = data;
        this.parity = parity;
        this.cursor = cursor;
        this.NumSubStripe = 0;
        this.NumDataBlock = NumDataBlock;
        this.NumParityBlock = NumParityBlock;
        this.Microec_buffer_size = 0;
        this.bindCoreidx = 11;
    }

    public EncodingTask() {
        this.codingLib = null;
        this.data = null;
        this.parity = null;
        this.cursor = null;
        this.NumSubStripe = 0;
        this.NumDataBlock = 0;
        this.NumParityBlock = 0;
        this.Microec_buffer_size = 0;
        this.bindCoreidx = 11;
    }

    public void setSubstripeAndBufferSize(int NumSubStripe, int Microec_buffer_size){
        this.NumSubStripe = NumSubStripe;
        this.Microec_buffer_size = Microec_buffer_size;
    }

    public void setEncodingBuffer(ByteBuffer[] data, ByteBuffer[] parity, ByteBuffer cursor) {
        this.data = data;
        this.parity = parity;
        this.cursor = cursor;
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        codingLib.MicroecEncoding(data, parity, cursor, NumSubStripe, NumDataBlock, NumParityBlock, Microec_buffer_size, bindCoreidx);
    }
}

