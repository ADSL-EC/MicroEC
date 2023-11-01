## Enabling Erasure Coding in Disaggregated Memory Systems

### 1. Introduction
Disaggregated memory (DM) separates compute and memory resources to build a huge memory pool. Erasure coding (EC) is expected to provide fault tolerance in DM with low memory cost. In DM with EC, objects are first coded in compute servers, then directly written to memory servers via high-speed networks like one-sided RDMA. However, as the one-sided RDMA latency goes down to the microsecond level, coding overhead degrades the performance in DM with EC.
To enable efficient EC in DM, we thoroughly analyze the coding stack from the perspective of cache efficiency and RDMA transmission. We develop MicroEC, which optimizes the coding workflow by reusing the auxiliary coding data and coordinates the coding and RDMA transmission with an exponential pipeline, as well as carefully adjusting the coding and transmission threads to minimize the latency.
We implement a prototype supporting common basic operations, such as write/read/degraded read/recovery. Experiments show that MicroEC reduces the write latency by up to 44.35% and 42.14% and achieves up to 1.80x and 1.73x write throughput, compared with the state-of-the-art DM systems with EC and 3-way replication for objects not smaller than 1MB, respectively.
For small objects, MicroEC also evidently reduces the variation of latency, e.g., it reduces the P99 latency of writing 1KB objects by 27.81%.

### 2. Overview
* MicroEC prototype is written in Java based on [Crail-1.3](https://github.com/apache/incubator-crail)
* MicroEC coding library is written in C based on [ISA-L](https://github.com/intel/isa-l)
* [Introduction to Crail](https://crail.incubator.apache.org/)

### 3. Requirements
* gcc >=4.7, nasm>= 2.15, make>=3.8
* Java >= 1.8 (Oracle JVMS have been tested)
* RDMA-based network, e.g., Infiniband, iWARP, RoCE.
* libdisni.so, available as part of [DiSNI](https://github.com/zrlio/disni)
* [Apache Maven 3.6.3](https://github.com/apache/maven/tree/maven-3.6.3)

### 4. Installation

* Get the source code of MicroEC.
```sh
$ git clone https://github.com/ADSL-EC/MicroEC.git
```

* MicroEC coding leverages [ISA-L(Intel(R) Intelligent Storage Acceleration Library](https://github.com/intel/isa-l/tree/v2.30.0) to perform fundamental coding computation. It provides a dynamically linked library *libmicroec.so*. The compilation commands are listed below.

```sh
# 1. intall isa-l
$ cd MicroEC/isa-l
$ ./autogen.sh
$ ./configure; make; sudo make install

# 2. compile for libmicroec.so
$ javac MicroEC/client/src/main/java/org/apache/crail/tools/Crailcoding.java
$ javah MicroEC/client/src/main/java/org.apache.crail.tools.Crailcoding
$ sudo cp MicroEC/client/src/main/java/org_apache_crail_tools_Crailcoding.h MicroEC/client/src/main/java/org/apache/crail/tools/
$ cd MicroEC/client/src/main/java/org/apache/crail/tools
$ g++ -std=c++11 -I/usr/java/jdk1.8.0_221/include -I/usr/java/jdk1.8.0_221/include/linux -fPIC -shared microec.c -o libmicroec.so -L.  /usr/lib/libisal.so
```

Then you will find `libmicroec.so` under the path `MicroEC/client/src/main/java/org/apache/crail/tools`.

* Compile MicroEC prototype.

```sh
$ cd MicroEC
$ mvn -DskipTests clean install
```

* Copy tarball from `assembly/target` to the cluster and unpack it using `tar xvfz apache-crail-1.3-incubating-SNAPSHOT-bin.tar.gz`.

### 5. Minimal Configuration for Testing the Prototype
* To configure Crail use the `*.template` files as a basis and modify it to match your environment. Set the `$CRAIL_HOME` environment variable to your Crail deployment’s path.
* There are 3 configuration files in the config folder.
#### crail-site.conf
This file describes the configuration of the file system, data tiers and RPC.

| **Property**                    | **Description**                                              |
| ------------------------------- | ------------------------------------------------------------ |
| crail.namenode.address          | Namenode hostname and port                                   |
| crail.cachepath                 | Hugepage path to client buffer cache                         |
| crail.cachelimit                | Size (byte) of client buffer cache                           |
| crail.storage.rdma.interface    | Network interface to bind to                                 |
| crail.storage.rdma.datapath     | Hugepage path to data                                        |
| crail.storage.rdma.storagelimit | Size (Bytes) of DRAM to provide, multiple of allocation size |
| crail.storage.types             | Storage tier types                                           |
| crail.namenode.rpctype          | Implementation of rpc                                        |
| crail.storage.replicationfactor | The number of slave replicas. For 3-way replication, the value is 2 |
| crail.storage.erasurecodingk    | k in (k,m) code                                              |
| crail.storage.erasurecodingm    | m in (k,m) code                                              |
| crail.storage.redundancytype    | The way to provide redundancy                                |

Here is an example.

```sh
crail.namenode.address            crail://node1:9060
crail.cachepath                   $CRAIL_HOME/tmp
crail.cachelimit                  32212254720
crail.storage.rdma.interface      ib0
crail.storage.rdma.datapath       $CRAIL_HOME/tmp
crail.storage.rdma.storagelimit   32212254720
crail.storage.types		  		 org.apache.crail.storage.rdma.RdmaStorageTier
crail.namenode.rpctype		  	 org.apache.crail.namenode.rpc.darpc.DaRPCNameNode
crail.storage.replicationfactor   2
crail.storage.erasurecodingk      4
crail.storage.erasurecodingm      2
crail.storage.redundancytype      erasurecoding
```

#### slaves

This file is used by the start/stop-crail.sh script to ease running/stopping Crail on multiple machines. Each line should contain a hostname where a storage tier is supposed to be started. Set the mapping relations from IPs to hostnames in /etc/hostname.

Here is an example.

```sh
node2
node3
node4
...
```

* Copy `libdisni.so` and `libmicroec.so` into `$CRAIL_HOME/lib/`


### 6. Testing the MicroEC Prototype
* Run the prototype
```sh
$ $CRAIL_HOME/bin/start-crail.sh
```
* Crail iobench
```sh
# write a 1MB object, repeat 1000 times for average
$ crail iobench -t writeMicroEC -s $((1024*1024)) -k $1000 -f /tmp.dat

# normal read, repeat 1000 times for average
$ crail iobench -t readMicroEC -k 1000 -f /tmp.dat

# degraded read, repeat 1000 times for average
$ crail iobench -t degradedReadMicroEC -k 1000 -f /tmp.dat

# recovery, repeat 1000 times for average
$ crail iobench -t recoveryMicroEC -k 1000 -f /tmp.dat
```
* Benchmark
```sh
# ycsb workload. For your testing convenience, we provide YCSB trace files in MicroEC/trace.
$ crail tracebench -t ycsb -s microec -p [path of your ycsb test file] -w [path of your ycsb warm file]

# IBM production workload. For your testing convenience, we provide IBM trace files in MicroEC/trace.
$ crail tracebench -t ibm -s microec -p [path of your ycsb test file] -w [path of your ycsb warm file]
```
* Stop the prototype
```sh
$ $CRAIL_HOME/bin/stop-crail.sh
```


### 7. Contact
* Please email to Qiliang Li (leeql@mail.ustc.edu.cn) and Liangliang Xu (llxu@mail.ustc.edu.cn) if you have any questions.