package org.opendedup.sdfs.io;

import java.util.concurrent.atomic.AtomicInteger;


import org.opendedup.hashing.Finger;

public abstract class AsyncChunkWriteActionListener {
	AtomicInteger dn = new AtomicInteger(0);
	AtomicInteger exdn = new AtomicInteger(0);
	int sz = 0;
	public abstract void commandException(Finger result,Throwable e);

	public abstract void commandResponse(Finger result);
	
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
	
	public int getMaxSz() {
		return this.sz;
	}
	
	public void setMaxSize(int sz) {
		this.sz = sz;
	}

}
