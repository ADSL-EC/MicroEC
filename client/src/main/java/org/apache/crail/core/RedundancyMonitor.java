package org.apache.crail.core;

import java.util.concurrent.ConcurrentHashMap;

public class RedundancyMonitor {
    // singleton
    private static final RedundancyMonitor instance=new RedundancyMonitor();
    public static ConcurrentHashMap<String, RedundancyType> redundancyFiles=new ConcurrentHashMap<>();

    private RedundancyMonitor(){
//        redundancyFiles=new ConcurrentHashMap<>();
    }

    public static RedundancyMonitor getInstance(){
        return instance;
    }
}