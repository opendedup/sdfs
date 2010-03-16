package org.opendedup.sdfs.io;

/**
 * 
 * @author Sam Silverberg This exception is thrown if a WritableCacheBuffer has
 *         already been closed for writing to a chunk store.
 */
public class BufferClosedException extends Exception {

	private static final long serialVersionUID = 1L;

	public BufferClosedException(String msg) {
		super(msg);
	}

}
