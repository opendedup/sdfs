package org.opendedup.sdfs.io;

import java.util.concurrent.atomic.AtomicInteger;

import org.opendedup.sdfs.io.WritableCacheBuffer.Shard;

public abstract class AsyncChunkReadActionListener {
	AtomicInteger dn = new AtomicInteger(0);
	AtomicInteger exdn = new AtomicInteger(0);

	public abstract void commandException(Exception e);

	public abstract void commandResponse(Shard result);

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

}
