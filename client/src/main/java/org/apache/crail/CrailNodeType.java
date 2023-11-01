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

package org.apache.crail;

public enum CrailNodeType {
//	DATAFILE(0), DIRECTORY(1), MULTIFILE(2), STREAMFILE(3), TABLE(4), KEYVALUE(5), REPLICASFILE(6), ERASURECODINGFILE(7);
	DATAFILE(0), DIRECTORY(1), MULTIFILE(2), STREAMFILE(3), TABLE(4), KEYVALUE(5);

	private int label;
	
	CrailNodeType(int label){
		this.label = label;
	}
	
	public int getLabel(){
		return this.label;
	}
	
	public boolean isDirectory(){
		return this == DIRECTORY;
	}
	
	public boolean isDataFile(){
		return this == DATAFILE;
	}	
	
	public boolean isMultiFile(){
		return this == MULTIFILE;
	}
	
	public boolean isStreamFile(){
		return this == STREAMFILE;
	}
	
	public boolean isTable() {
		return this == TABLE;
	}

	public boolean isKeyValue() {
		return this == KEYVALUE;
	}

//	public boolean isReplicasFile() {
//		return this == REPLICASFILE;
//	}
//
//	public boolean isErasureCodingFile() {
//		return this == ERASURECODINGFILE;
//	}
	
	public boolean isContainer(){
		return this == DIRECTORY || this == MULTIFILE || this == TABLE;
	}	
	
	public static CrailNodeType parse(int label) {
		for (CrailNodeType val : CrailNodeType.values()) {
			if (val.getLabel() == label) {
				return val;
			}
		}
		throw new IllegalArgumentException();
	}

}
