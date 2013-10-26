package org.opendedup.mtools;

public class FDiskException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public FDiskException(Exception e) {
		super(e);
	}
	
	public FDiskException(String e) {
		super(e);
	}

}
