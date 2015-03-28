package org.opendedup.util;


public abstract class AsyncChunkWriteActionListener {

	public abstract void commandException(Throwable e);

	public abstract void commandResponse();
	

}
