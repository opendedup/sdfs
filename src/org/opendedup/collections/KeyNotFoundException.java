package org.opendedup.collections;

import org.opendedup.util.StringUtils;

public class KeyNotFoundException extends Exception {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3838655007053133611L;

	public KeyNotFoundException(){
		super();
	}
	
	public KeyNotFoundException(byte [] key){
		super("Key [" + StringUtils.getHexString(key) + "] not found");
	}

}
