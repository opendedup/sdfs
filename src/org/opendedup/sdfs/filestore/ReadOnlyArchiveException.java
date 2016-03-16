package org.opendedup.sdfs.filestore;

public class ReadOnlyArchiveException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7877745895864932265L;

	public ReadOnlyArchiveException() {
		super();
	}

	public ReadOnlyArchiveException(String msg) {
		super(msg);
	}

}
