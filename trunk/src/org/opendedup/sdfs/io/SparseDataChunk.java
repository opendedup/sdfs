package org.opendedup.sdfs.io;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.hashing.HashFunctionPool;

public class SparseDataChunk implements Externalizable {

	private boolean doop;
	private byte[] hash;
	private boolean localData = false;
	int currentpos = 1;
	private int RAWDL = 1 + HashFunctionPool.hashLength + 1 + 8 + 8;
	private byte[] hashlocs;
	private long fpos;
	private byte version = 1;
	private long timestamp = 0;
	
	public SparseDataChunk() {
		
	}

	public SparseDataChunk(byte[] rawData, byte version) throws IOException {
		this.version = version;
		if(version == 0)
			this.RAWDL = LongByteArrayMap._FREE.length;
		else if(version ==1)
			this.RAWDL = LongByteArrayMap._V1FREE.length;
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
		hashlocs = new byte[8];
		buf.get(hashlocs);
		if(this.version > 0) {
			this.version = buf.get();
			this.timestamp = buf.getLong();
		}
	}

	public SparseDataChunk(boolean doop, byte[] hash, boolean localData,
			byte[] hashlocs,byte version,long timestamp) {
		this.version = version;
		if(version == 0)
			this.RAWDL = LongByteArrayMap._FREE.length;
		else if(version ==1)
			this.RAWDL = LongByteArrayMap._V1FREE.length;
		this.doop = doop;
		this.hash = hash;
		this.localData = localData;
		this.hashlocs = hashlocs;
		this.timestamp = timestamp;
	}

	public boolean isDoop() {
		return doop;
	}

	public byte[] getHash() {
		return hash;
	}

	public void setHashLoc(byte[] hashlocs) {
		this.currentpos = 1;
		this.hashlocs = hashlocs;
	}

	public byte[] getBytes() {
		ByteBuffer buf = ByteBuffer.wrap(new byte[RAWDL]);
		if (doop)
			buf.put((byte) 1);
		else
			buf.put((byte) 0);
		buf.put(this.hash);
		if (localData)
			buf.put((byte) 1);
		else
			buf.put((byte) 0);
		buf.put(this.hashlocs);
		if(this.version >0) {
			buf.putShort(this.version);
			buf.putLong(this.timestamp);
		}
		return buf.array();
	}

	public boolean isLocalData() {
		return localData;
	}

	public void setLocalData(boolean local) {
		this.localData = local;
	}

	public byte[] getHashLoc() {
		if (this.hashlocs[1] > 0)
			this.hashlocs[0] = 1;
		return this.hashlocs;
	}

	public synchronized void addHashLoc(byte loc) {
		if (currentpos < this.hashlocs.length) {
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
		hashlocs = new byte[8];
		hashlocs[0] = -1;
		currentpos = 1;
	}
	
	public void setDoop(boolean doop) {
		if(this.doop) {
			this.doop = doop;
			this.hashlocs[0] =1;
		}
	}

	public long getFpos() {
		return fpos;
	}

	public void setFpos(long fpos) {
		this.fpos = fpos;
	}

	@Override
	public void readExternal(ObjectInput arg0) throws IOException,
			ClassNotFoundException {
		this.fpos = arg0.readLong();
		this.doop = arg0.readBoolean();
		this.currentpos = arg0.readInt();
		this.localData = arg0.readBoolean();
		this.hash = new byte [HashFunctionPool.hashLength];
		arg0.read(hash);
		this.hashlocs = new byte[8];
		arg0.readFully(hashlocs);
		if(arg0.available() > 0) {
			this.version = arg0.readByte();
			this.timestamp = arg0.readLong();
		}
	}

	@Override
	public void writeExternal(ObjectOutput arg0) throws IOException {
		arg0.writeLong(this.fpos);
		arg0.writeBoolean(doop);
		arg0.writeInt(currentpos);
		arg0.writeBoolean(this.localData);
		arg0.write(hash);
		arg0.write(hashlocs);
		if(this.version > 0) {
			arg0.writeByte(this.version);
			arg0.writeLong(timestamp);
		}
	}

}
