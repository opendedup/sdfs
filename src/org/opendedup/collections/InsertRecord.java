package org.opendedup.collections;

import com.google.common.primitives.Longs;

public class InsertRecord {
	final long pos;
	final boolean inserted;
	boolean remote = false;
	
	public InsertRecord(long pos, boolean inserted) {
		this.pos = pos;
		this.inserted = inserted;
	}
	
	public boolean isInserted() {
		return this.inserted;
	}
	
	public long getPos() {
		return this.pos;
	}
	
	public byte [] getHashLoc() {
		return Longs.toByteArray(pos);
	}
	
	public boolean isRemote() {
		return this.remote;
	}
	
	public InsertRecord setRemote(boolean remote) {
		this.remote = remote;
		return this;
	}

}
