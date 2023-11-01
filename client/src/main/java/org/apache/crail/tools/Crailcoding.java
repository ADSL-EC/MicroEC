package org.apache.crail.tools;

import java.nio.ByteBuffer;


public class Crailcoding {

    // warm used
    public native void WarmCall();

    // init auxiliaries
    public native void InitAuxiliaries(int NumDataBlock, int NumParityBlock, ByteBuffer encodingMatrix, ByteBuffer gTbls);
    // test init auxiliaries
    public native int TestInitAuxiliaries(int NumDataBlock, int NumParityBlock, ByteBuffer encodingMatrix, ByteBuffer gTbls);

    //Encoding APIs
    public native void NativeEncoding(ByteBuffer[] data, ByteBuffer[] parity, int NumDataBlock, int NumParityBlock, int Naive_buffer_size, int NumCore);

    public native void MicroecEncoding(ByteBuffer[] data, ByteBuffer[] parity, ByteBuffer cursor, int NumSubStripe, int NumDataBlock, int NumParityBlock, int Microec_buffer_size, int NumCore);

    //For Breakdown
    public native void PureMicroecEncoding(ByteBuffer[] data, ByteBuffer[] parity, int NumSubStripe, int NumDataBlock, int NumParityBlock, int Microec_buffer_size, int NumCore);

    //Decoding APIs
    public native void NativeDecoding(ByteBuffer[] Src, ByteBuffer[] EraseData, int[] SrcIndex, int[] EraseIndex, int NumDataBlock, int NumParityBlock, int Naive_buffer_size, int NumEraseData, int NumCore);

    public native void MicroecDecoding(ByteBuffer[] Src, ByteBuffer[] EraseData, int[] SrcIndex, int[] EraseIndex, ByteBuffer cursor, int NumSubStripe, int NumDataBlock, int NumParityBlock, int Microec_buffer_size, int NumEraseData, int NumCore, ByteBuffer bufHasTransfered, ByteBuffer bufHasCoded);

    static {
        System.loadLibrary("microec");
    }

    public Crailcoding(int k, int p) {
//        System.out.println("MicroEC Coding Ready");
    }

}
