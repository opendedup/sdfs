package org.opendedup.sdfs.network;

public interface AsyncCmdListener {
	public abstract void commandException(Exception e) ;
	public abstract void commandResponse(Object result,HashClient client);

}
