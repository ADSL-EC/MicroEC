package org.apache.crail.tools;

import java.nio.ByteBuffer;

public class DecodingTask extends CodingTask implements Runnable {

    ByteBuffer[] eraseData;
    int[] srcIndex;
    int[] eraseIndex;
    int numEraseData;
    ByteBuffer bufHasTransfered;
    ByteBuffer bufHasCoded;

    public DecodingTask(Crailcoding codingLib, int NumSubStripe, int NumDataBlock, int NumParityBlock, int Microec_buffer_size, int[] srcIndex, int[] eraseIndex, int numEraseData) {
        this.codingLib = codingLib;
        this.data = null;
        this.eraseData = null;
        this.cursor = null;
        this.NumSubStripe = NumSubStripe;
        this.NumDataBlock = NumDataBlock;
        this.NumParityBlock = NumParityBlock;
        this.Microec_buffer_size = Microec_buffer_size;
        this.srcIndex = srcIndex;
        this.eraseIndex = eraseIndex;
        this.numEraseData = numEraseData;
        this.bufHasTransfered = null;
        this.bufHasCoded = null;
    }

    public DecodingTask(Crailcoding codingLib, ByteBuffer[] data, ByteBuffer[] eraseData, ByteBuffer cursor, int NumDataBlock, int NumParityBlock, int[] srcIndex, int[] eraseIndex, int numEraseData, ByteBuffer bufHasTransfered, ByteBuffer bufHasCoded) {
        this.codingLib = codingLib;
        this.data = data;
        this.eraseData = eraseData;
        this.cursor = cursor;
        this.NumSubStripe = 0;
        this.NumDataBlock = NumDataBlock;
        this.NumParityBlock = NumParityBlock;
        this.Microec_buffer_size = 0;
        this.srcIndex = srcIndex;
        this.eraseIndex = eraseIndex;
        this.numEraseData = numEraseData;
        this.bufHasTransfered = bufHasTransfered;
        this.bufHasCoded = bufHasCoded;
    }

    public void setSubstripeAndBufferSize(int NumSubStripe, int Microec_buffer_size){
        this.NumSubStripe = NumSubStripe;
        this.Microec_buffer_size = Microec_buffer_size;
    }

    public void setDecodingBuffer(ByteBuffer[] data, ByteBuffer[] eraseData, ByteBuffer cursor) {
        this.data = data;
        this.eraseData = eraseData;
        this.cursor = cursor;
    }

    public void setCodingCounter(ByteBuffer bufHasTransfered, ByteBuffer bufHasCoded) {
        this.bufHasTransfered = bufHasTransfered;
        this.bufHasCoded = bufHasCoded;
    }

    @Override
    public void run() {
        codingLib.MicroecDecoding(data, eraseData, srcIndex, eraseIndex, cursor, NumSubStripe, NumDataBlock, NumParityBlock, Microec_buffer_size, numEraseData, bindCoreidx, bufHasTransfered, bufHasCoded);
    }
}
