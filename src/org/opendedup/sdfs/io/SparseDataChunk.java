package org.opendedup.sdfs.io;

import java.io.IOException;

import java.nio.ByteBuffer;

import org.opendedup.hashing.HashFunctionPool;

public class SparseDataChunk {

	private boolean doop;
	private byte[] hash;
	private boolean localData = false;
	//private long timeAdded = 0;
	private byte [] hashlocs;
	public static final int RAWDL = 1 + HashFunctionPool.hashLength + 1 + 8;

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
		buf.put(this.hashlocs);
		return buf.array();
	}

	public boolean isLocalData() {
		return localData;
	}

	public void setLocalData(boolean local) {
		this.localData = local;
	}
	
	public byte [] getHashLoc() {
		return this.hashlocs;
	}

}
