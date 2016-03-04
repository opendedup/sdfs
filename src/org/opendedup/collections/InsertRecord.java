package org.opendedup.collections;

import com.google.common.primitives.Longs;

public class InsertRecord {
	private boolean inserted;
	private byte [] hashlocs;
	
	public InsertRecord(boolean inserted, long pos) {
		this.inserted = inserted;
		this.hashlocs = Longs.toByteArray(pos);
	}
	
	public InsertRecord(boolean inserted, byte [] locs) {
		this.inserted = inserted;
		this.hashlocs = locs;
	}
	
	public boolean getInserted() {
		return this.inserted;
	}
	
	public void setHashLocs(byte [] hashlocs) {
		this.hashlocs = hashlocs;
	}
	
	public byte[] getHashLocs() {
		return this.hashlocs;
	}

}
