package org.opendedup.sdfs.io;

import java.io.IOException;
import java.io.Serializable;

import java.nio.ByteBuffer;

import org.opendedup.hashing.HashFunctionPool;


public class SparseDataChunk implements Serializable {


	private static final long serialVersionUID = 2355100719332694545L;
	private boolean doop;
	private byte[] hash;
	private boolean localData = false;
	int currentpos = 1;
	public static final int RAWDL = 1 + HashFunctionPool.hashLength + 1 + 8;
	private byte [] hashlocs;
	private long fpos;

	public SparseDataChunk(byte[] rawData) throws IOException {
		if (rawData.length != RAWDL)
			throw new IOException(
					"possible data corruption: byte array length "
							+ rawData.length + " does not equal " + RAWDL);
		ByteBuffer buf = ByteBuffer.wrap(rawData);
		byte b = buf.get();
		if (b == 0)
			doop = false;
		else
			doop = true;
		hash = new byte[HashFunctionPool.hashLength];
		buf.get(hash);
		b = buf.get();
		if (b == 0)
			this.localData = false;
		else
			this.localData = true;
		hashlocs = new byte [8];
		buf.get(hashlocs);
	}

	public SparseDataChunk(boolean doop, byte[] hash, boolean localData,
			byte [] hashlocs) {
		this.doop = doop;
		this.hash = hash;
		this.localData = localData;
		this.hashlocs = hashlocs;
	}

	public boolean isDoop() {
		return doop;
	}

	public byte[] getHash() {
		return hash;
	}
	
	public void setHashLoc(byte [] hashlocs) {
		this.currentpos = 1;
		this.hashlocs = hashlocs;
	}

	public byte[] getBytes() {
		ByteBuffer buf = ByteBuffer.wrap(new byte[RAWDL]);
		if (doop)
			buf.put((byte) 1);
		else
			buf.put((byte) 0);
		buf.put(hash);
		if (localData)
			buf.put((byte) 1);
		else
			buf.put((byte) 0);
		buf.put(hashlocs);
		return buf.array();
	}

	public boolean isLocalData() {
		return localData;
	}

	public void setLocalData(boolean local) {
		this.localData = local;
	}
	
	public byte [] getHashLoc() {
		if(this.hashlocs[1] > 0)
			this.hashlocs[0] = 0;
		return this.hashlocs;
	}
	
	public synchronized void addHashLoc(byte loc) {
		if(currentpos <this.hashlocs.length) {
			this.hashlocs[currentpos] = loc;
			currentpos++;
		}
	}
	
	public int getCopies() {
		int ncopies = 0;
		for (int i = 1; i < 8; i++) {
			if (hashlocs[i] > (byte) 0) {
				ncopies++;
			}
		}
		return ncopies;
	}
	
	public void resetHashLoc() {
		hashlocs = new byte [8];
		hashlocs[0] = -1;
		currentpos=1;
	}

	public long getFpos() {
		return fpos;
	}

	public void setFpos(long fpos) {
		this.fpos = fpos;
	}

}
