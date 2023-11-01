/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.crail.core;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.crail.CrailNode;
import org.apache.crail.CrailNodeType;
import org.apache.crail.Upcoming;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.rpc.RpcCreateFile;
import org.apache.crail.rpc.RpcDeleteFile;
import org.apache.crail.rpc.RpcErrors;
import org.apache.crail.rpc.RpcGetFile;
import org.apache.crail.rpc.RpcRenameFile;
import org.apache.crail.rpc.RpcVoid;

public abstract class CoreMetaDataOperation<R,T> implements Upcoming<T> {
	protected static int RPC_PENDING = 0;
	protected static int RPC_DONE = 1;
	protected static int RPC_ERROR = 2;		
	
	private AtomicInteger status;
	protected Future<R> rpcResult;
	private T finalResult;
	private Exception exception;
	
	abstract T process(R tmp) throws Exception;
	
	public CoreMetaDataOperation(Future<R> result){
		this.rpcResult = result;
		this.finalResult = null;
		this.status = new AtomicInteger(RPC_PENDING);
		this.exception = null;
	}

	// add getStatus
	public AtomicInteger getStatus() {
		return status;
	}

	@Override
	public boolean isDone() {
		if (status.get() == RPC_PENDING){
			try {
				if (rpcResult.isDone()){
					R tmp = rpcResult.get();
					finalResult = process(tmp);
					status.set(RPC_DONE);
				}
			} catch (Exception e) {
				status.set(RPC_ERROR);
				this.exception = e;
			}
		}
		
		return status.get() > 0;
	}

	@Override
	public T get() throws InterruptedException, ExecutionException {
//		long getTmpStart=0,getTmpEnd=0,processStart=0,processEnd=0,setStart=0,setEnd=0;
//		long start=System.nanoTime();
		if (this.exception != null){
			throw new ExecutionException(exception);
		}

//		System.out.println("get-1");
		if (status.get() == RPC_PENDING){
			try {
//				getTmpStart=System.nanoTime();
				R tmp = rpcResult.get();
//				getTmpEnd=System.nanoTime();

//				System.out.println("get-2");

//				processStart=System.nanoTime();
				finalResult = process(tmp);
//				processEnd=System.nanoTime();

//				System.out.println("get-3");

//				setStart=System.nanoTime();
				status.set(RPC_DONE);
//				setEnd=System.nanoTime();

//				System.out.println("get-4");
			} catch (Exception e) {
				status.set(RPC_ERROR);
				this.exception = e;
			}
		}	
		
		if (status.get() == RPC_DONE){
//			long end=System.nanoTime();

//			System.out.println("CoreMetaDataOperation get tmp time:"+(getTmpEnd-getTmpStart));
//			System.out.println("CoreMetaDataOperation get process time:"+(processEnd-processStart));
//			System.out.println("CoreMetaDataOperation get setStatus time:"+(setEnd-setStart));
//			System.out.println("CoreMetaDataOperation all get time:"+(end-start));
			return finalResult;
		} else if (status.get() == RPC_PENDING){
			throw new InterruptedException("RPC timeout");
		} else if (exception != null) {
			throw new ExecutionException(exception);
		} else {
			throw new InterruptedException("RPC error");
		}	
	}

	@Override
	public T get(long timeout, TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException {
		if (this.exception != null){
			throw new ExecutionException(exception);
		}		
		
		if (status.get() == RPC_PENDING){
			try {
				R tmp = rpcResult.get(CrailConstants.DATA_TIMEOUT, TimeUnit.MILLISECONDS);
				finalResult = process(tmp);
				status.set(RPC_DONE);
			} catch (Exception e) {
				status.set(RPC_ERROR);
				this.exception = e;
			}
		}	
		
		if (status.get() == RPC_DONE){
			return finalResult;
		} else if (status.get() == RPC_PENDING){
			throw new InterruptedException("RPC timeout");
		} else if (exception != null) {
			throw new ExecutionException(exception);
		} else {
			throw new InterruptedException("RPC error");
		}
	}
	
	public T early() throws Exception {
		return this.get();
	}
	
	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}	
}

class CreateReplicasNodeFuture extends CoreMetaDataOperation<RpcCreateFile, CrailNode> {
	private CoreDataStore fs;
	private String path;
	private CrailNodeType type;

	private String[] slavesPath;
	private CreateNodeFuture[] slavesFuture;

	private RedundancyType redundancyType;

	public CreateReplicasNodeFuture(CoreDataStore fs, String path, CrailNodeType type, Future<RpcCreateFile> primaryFileRes, List<Future<RpcCreateFile>> slavesFileRes, RedundancyType redundancyType) {
		super(primaryFileRes);
		this.fs=fs;
		this.path=path;
		this.type=type;
		this.redundancyType=redundancyType;

        slavesPath=new String[CrailConstants.REPLICATION_FACTOR];
		slavesFuture=new CreateNodeFuture[CrailConstants.REPLICATION_FACTOR];
		for(int i=0;i<CrailConstants.REPLICATION_FACTOR;i++){
			slavesPath[i]=path+"LQL"+i;
			slavesFuture[i]=new CreateNodeFuture(fs,slavesPath[i],type,slavesFileRes.get(i));
		}
	}

	@Override
	CrailNode process(RpcCreateFile primaryResponse) throws Exception {
//		System.out.println("CreateReplicasNodeFuture-process-1");
		RpcCreateFile[] slavesResponse=new RpcCreateFile[CrailConstants.REPLICATION_FACTOR];
		for(int i=0;i<CrailConstants.REPLICATION_FACTOR;i++){
			if(slavesFuture[i].getStatus().get()==RPC_PENDING){
				slavesResponse[i]=slavesFuture[i].rpcResult.get();
			}
		}
//		System.out.println("CreateReplicasNodeFuture-process-2");

		return fs._createReplicasNode(path, slavesPath, type, primaryResponse, slavesResponse, redundancyType);
	}
}

class CreateNodeFuture extends CoreMetaDataOperation<RpcCreateFile, CrailNode> {
	private CoreDataStore fs;
	private String path;
	private CrailNodeType type;

	public CreateNodeFuture(CoreDataStore fs, String path, CrailNodeType type, Future<RpcCreateFile> fileRes) {
		super(fileRes);
		this.fs = fs;
		this.path = path;
		this.type = type;
	}

	@Override
	CrailNode process(RpcCreateFile response) throws Exception {
		return fs._createNode(path, type, response);
	}

	@Override
	public CrailNode early() throws Exception {
		switch(type){
		case DATAFILE:
			return new CoreEarlyFile(fs, path, type, this);
		case DIRECTORY:
		case MULTIFILE:
			return null;
		default:
			return super.early();
		}
	}

}

class LookupNodeFuture extends CoreMetaDataOperation<RpcGetFile, CrailNode> {
	private String path;
	private CoreDataStore fs;	

	public LookupNodeFuture(CoreDataStore fs, String path, Future<RpcGetFile> fileRes) {
		super(fileRes);
		this.fs = fs;
		this.path = path;
	}

	@Override
	CrailNode process(RpcGetFile tmp) throws Exception {
		return fs._lookupNode(tmp, path);
	}

}

class DeleteNodeFuture extends CoreMetaDataOperation<RpcDeleteFile, CrailNode> {
	private String path;
	private boolean recursive;
	private CoreDataStore fs;

	public DeleteNodeFuture(CoreDataStore fs, String path, boolean recursive, Future<RpcDeleteFile> fileRes) {
		super(fileRes);
		this.fs = fs;
		this.path = path;
		this.recursive = recursive;
	}

	@Override
	CrailNode process(RpcDeleteFile tmp) throws Exception {
		return fs._delete(tmp, path, recursive);
	}
}

class RenameNodeFuture extends CoreMetaDataOperation<RpcRenameFile, CrailNode> {
	private String src;
	private String dst;
	private CoreDataStore fs;

	public RenameNodeFuture(CoreDataStore fs, String src, String dst, Future<RpcRenameFile> fileRes) {
		super(fileRes);
		this.fs = fs;
		this.src = src;
		this.dst = dst;
	}

	@Override
	CrailNode process(RpcRenameFile tmp) throws Exception {
		return fs._rename(tmp, src, dst);
	}
}

class SyncNodeFuture extends CoreMetaDataOperation<RpcVoid, Void> {

	public SyncNodeFuture(Future<RpcVoid> fileRes) {
		super(fileRes);
	}
	
	@Override
	Void process(RpcVoid tmp) throws Exception {
		if (tmp.getError() != RpcErrors.ERR_OK){
			throw new Exception("sync: " + RpcErrors.messages[tmp.getError()]);
		}
		return null;
	}
}

class NoOperation implements Future<Void> {

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return true;
	}

	@Override
	public Void get() throws InterruptedException, ExecutionException {
		return null;
	}

	@Override
	public Void get(long timeout, TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException {
		return null;
	}
}


