package org.opendedup.sdfs.io;

import java.io.Externalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.sdfs.Main;

public class SparseDataChunk implements Externalizable {

	private int doop;
	private byte[] hash;
	private boolean localData = false;
	int currentpos = 1;
	private int RAWDL = 4 + ((HashFunctionPool.hashLength + 8) * HashFunctionPool.max_hash_cluster);
	// private int RAWDL;
	private byte[] hashlocs;
	private long fpos;
	private byte version = 1;
	private static final long serialVersionUID = -2782607786999940224L;

	public SparseDataChunk() {

	}

	public SparseDataChunk(byte[] rawData) throws IOException {
		if (rawData.length == LongByteArrayMap._FREE.length)
			this.version = 0;
		if (rawData.length == LongByteArrayMap._V1FREE.length)
			this.version = 1;
		if (rawData.length == LongByteArrayMap._V2FREE.length)
			this.version = 2;
		if (version == 0)
			this.RAWDL = LongByteArrayMap._FREE.length;
		else if (version == 1)
			this.RAWDL = LongByteArrayMap._V1FREE.length;
		else if (version == 2)
			this.RAWDL = LongByteArrayMap._V2FREE.length;
		if (rawData.length != RAWDL)
			throw new IOException(
					"possible data corruption: byte array length "
							+ rawData.length + " does not equal " + RAWDL);

		ByteBuffer buf = ByteBuffer.wrap(rawData);
		if (this.version == 0) {
			byte b = buf.get();
			if (b == 0)
				doop = 0;
			else
				doop = Main.CHUNK_LENGTH;
			hash = new byte[HashFunctionPool.hashLength];
			buf.get(hash);
			b = buf.get();
			if (b == 0)
				this.localData = false;
			else
				this.localData = true;
			hashlocs = new byte[8];
			buf.get(hashlocs);
		} else if(this.version == 1) {
			doop = buf.getInt();
			hash = new byte[HashFunctionPool.hashLength
					* HashFunctionPool.max_hash_cluster];
			buf.get(hash);
			hashlocs = new byte[8 * HashFunctionPool.max_hash_cluster];
			buf.get(hashlocs);
		}
		else if(this.version == 2) {
			doop = buf.getInt();
			hash = new byte[buf.getInt()];
			buf.get(hash);
			hashlocs = new byte[buf.getInt()];
			buf.get(hashlocs);
		}
	}

	public SparseDataChunk(int doop, byte[] hash, boolean localData,
			byte[] hashlocs, byte version) {
		this.version = version;
		if (version == 0)
			this.RAWDL = LongByteArrayMap._FREE.length;
		else if (version == 1)
			this.RAWDL = LongByteArrayMap._V1FREE.length;
		else if (version == 2)
			this.RAWDL = LongByteArrayMap._V2FREE.length;
		this.doop = doop;
		this.hash = hash;
		this.localData = localData;
		this.hashlocs = hashlocs;
	}

	public int getDoop() {
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
		ByteBuffer buf = null;
		if (this.version == 1) {
			 buf = ByteBuffer.wrap(new byte[4+hash.length+hashlocs.length]);
			buf.putInt(this.doop);
			buf.put(this.hash);
			buf.put(this.hashlocs);
		} else if(this.version == 2) {
			buf = ByteBuffer.wrap(new byte[4+4+hash.length+4+hashlocs.length]);
			buf.putInt(this.doop);
			buf.putInt(this.hash.length);
			buf.put(this.hash);
			buf.putInt(this.hashlocs.length);
			buf.put(this.hashlocs);
		}
		else {
			buf = ByteBuffer.wrap(new byte[1+hash.length+1+hashlocs.length]);
			if (doop > 0)
				buf.put((byte) 1);
			else
				buf.put((byte) 0);

			buf.put(this.hash);
			if (localData)
				buf.put((byte) 1);
			else
				buf.put((byte) 0);
			buf.put(this.hashlocs);
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
		if (this.version == 0) {
			if (this.hashlocs[1] > 0)
				this.hashlocs[0] = 1;
		}
		return this.hashlocs;
	}

	public synchronized void addHashLoc(byte loc) throws IOException {
		if (this.version > 0)
			throw new IOException("Not Supported int this version");
		if (currentpos < this.hashlocs.length) {
			this.hashlocs[currentpos] = loc;
			currentpos++;
		}
	}

	public int getCopies() throws IOException {
		if (this.version > 0)
			throw new IOException("Not Supported int this version");
		int ncopies = 0;
		for (int i = 1; i < 8; i++) {
			if (hashlocs[i] > (byte) 0) {
				ncopies++;
			}
		}
		return ncopies;
	}

	public void resetHashLoc() throws IOException {
		if (this.version > 0)
			throw new IOException("Not Supported int this version");
		hashlocs = new byte[8];
		hashlocs[0] = -1;
		currentpos = 1;
	}

	public void setDoop(int doop) {
		if (this.version == 0) {
			this.doop = doop;
			if (this.doop > 0) {
				this.hashlocs[0] = 1;
			}
		} else
			this.doop = doop;
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
		this.doop = arg0.readInt();
		this.currentpos = arg0.readInt();
		this.localData = arg0.readBoolean();
		this.hash = new byte[HashFunctionPool.hashLength];
		arg0.read(hash);
		this.hashlocs = new byte[8];
		arg0.readFully(hashlocs);
		if (arg0.available() > 0) {
			this.version = arg0.readByte();
		}
	}

	@Override
	public void writeExternal(ObjectOutput arg0) throws IOException {
		arg0.writeLong(this.fpos);
		arg0.writeInt(doop);
		arg0.writeInt(currentpos);
		arg0.writeBoolean(this.localData);
		arg0.write(hash);
		arg0.write(hashlocs);
		if (this.version > 0) {
			arg0.writeByte(this.version);
		}
	}

	public List<HashLocPair> getFingers() {
		ArrayList<HashLocPair> al = new ArrayList<HashLocPair>();
		ByteBuffer hb = ByteBuffer.wrap(this.getHash());
		ByteBuffer hl = ByteBuffer.wrap(this.hashlocs);
		for (int i = 0; i < HashFunctionPool.max_hash_cluster; i++) {
			byte[] _hash = new byte[HashFunctionPool.hashLength];
			byte[] _hl = new byte[8];
			hl.get(_hl);

			hb.get(_hash);
			if (HashFunctionPool.max_hash_cluster == 1 || _hl[1] != 0) {
				HashLocPair p = new HashLocPair();
				p.hash = _hash;
				p.hashloc = _hl;
				al.add(p);
			} else
				break;
		}
		return al;
	}

	public static class HashLocPair {
		public byte[] hash;
		public byte[] hashloc;
	}

}
