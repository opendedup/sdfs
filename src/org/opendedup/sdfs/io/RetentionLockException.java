package org.opendedup.sdfs.io;

public class RetentionLockException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public MetaDataDedupFile mf;
	
	public RetentionLockException(MetaDataDedupFile mf){
		this.mf = mf;
	}

}
