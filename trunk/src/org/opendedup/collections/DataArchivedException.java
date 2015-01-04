package org.opendedup.collections;

public class DataArchivedException extends Exception {
	long pos;
	byte [] hash;

	/**
	 * 
	 */
	private static final long serialVersionUID = 4180645903735154438L;
	
	public DataArchivedException(long pos,byte[] hash) {
		this.pos = pos;
		this.hash = hash;
	}
	
	public long getPos() {
		return pos;
	}
	
	public byte [] getHash() {
		return hash;
	}

}
