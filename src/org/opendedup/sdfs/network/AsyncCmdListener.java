package org.opendedup.sdfs.network;

public interface AsyncCmdListener {
	
	public abstract HashClientPool getPool();
	public abstract void setPool(HashClientPool pool);
	
	public abstract void commandException(Exception e);

	public abstract void commandResponse(Object result, HashClient client);

}
