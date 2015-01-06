package org.opendedup.sdfs.io;


import java.util.concurrent.atomic.AtomicInteger;

import org.opendedup.collections.DataArchivedException;
import org.opendedup.sdfs.io.WritableCacheBuffer.Shard;

public abstract class AsyncChunkReadActionListener {
	private DataArchivedException dar = null;
	AtomicInteger exdn = new AtomicInteger(0);
	AtomicInteger dn = new AtomicInteger(0);
	

	public abstract void commandException(Exception e);
	
	public abstract void commandResponse(Shard result);

	public abstract void commandArchiveException(DataArchivedException e);

	public int incrementandGetDN() {
		return dn.incrementAndGet();
	}

	public int getDN() {
		return dn.get();
	}

	public int incrementAndGetDNEX() {
		return exdn.incrementAndGet();
	}

	public int getDNEX() {
		return exdn.get();
	}
	
	public synchronized void setDAR(DataArchivedException dar) {
		if(this.dar == null)
			this.dar = dar;
	}
	
	public synchronized DataArchivedException getDAR() {
		return this.dar;
	}

}
