package org.opendedup.sdfs.io;

import java.util.List;

import org.opendedup.hashing.Finger;

public class MatchException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	List<Finger> fs;
	
	public MatchException(List<Finger> fs)  {
		this.fs = fs;
	}

}
