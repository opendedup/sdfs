package org.opendedup.sdfs.filestore.cloud;

import java.util.concurrent.atomic.AtomicInteger;

import org.opendedup.collections.DataArchivedException;

public abstract class AbstractDownloadListener {
	AtomicInteger dn = new AtomicInteger(0);
	AtomicInteger exdn = new AtomicInteger(0);
	private DataArchivedException dar = null;

	public abstract void commandException(Exception e);

	public abstract void commandResponse(DownloadShard shard);

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
		if (this.dar == null)
			this.dar = dar;
	}

	public synchronized DataArchivedException getDAR() {
		return this.dar;
	}

}
