package org.opendedup.collections;

import java.io.IOException;

public class HashExistsException extends IOException {

	private static final long serialVersionUID = 2207515169199626140L;
	private long pos;
	private byte [] hash;
	
	public HashExistsException(long pos, byte [] hash) {
		this.pos = pos;
		this.hash = hash;
	}
	
	public long getPos() {
		return this.pos;
	}
	
	public byte [] getHash() {
		return this.hash;
	}
	

}
