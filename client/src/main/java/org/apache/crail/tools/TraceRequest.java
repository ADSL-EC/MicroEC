package org.apache.crail.tools;

public class TraceRequest {
    public String fileName;
    public String requestType;
    public int size;
    public boolean isDegradeRead;

    public TraceRequest(String fileName, String requestType, int size, boolean isDegradeRead) {
        this.fileName = fileName;
        this.requestType = requestType;
        this.size = size;
        this.isDegradeRead = isDegradeRead;
    }
}
