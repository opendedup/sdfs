package org.opendedup.collections;

public class DataArchivedException extends Exception {
	long pos = -1;
	byte [] hash = null;

	/**
	 * 
	 */
	private static final long serialVersionUID = 4180645903735154438L;
	
	public DataArchivedException() {
		
	}
	
	public DataArchivedException(long pos,byte[] hash) {
		this.pos = pos;
		this.hash = hash;
	}
	
	public void setPos(long l) {
		this.pos = l;
	}
	
	public void setHash(byte [] h) {
		this.hash = h;
	}
	
	public long getPos() {
		return pos;
	}
	
	public byte [] getHash() {
		return hash;
	}

}
