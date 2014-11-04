package org.opendedup.sdfs.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.opendedup.hashing.HashFunctionPool;

public class HashLocPair implements Comparable<HashLocPair> {
	public static final int BAL = HashFunctionPool.hashLength + 8 + 4 + 4+4+4;
	public byte[] hash;
	public byte[] hashloc;
	public int len;
	public int pos;
	public int offset;
	public int nlen;
	public boolean np;
	public byte[] asArray() throws IOException {
		ByteBuffer bf = ByteBuffer.wrap(new byte[BAL]);
		bf.put(hash);
		bf.put(hashloc);
		bf.putInt(len);
		bf.putInt(pos);
		bf.putInt(offset);
		bf.putInt(nlen);
		this.checkCorrupt();
		return bf.array();
	}
	
	private void checkCorrupt () throws IOException {
		if(len <0 || pos <0 || offset <0 || nlen < 0)
			throw new IOException("data is corrupt " +this);
	}
	
	public boolean isInvalid() {
		return (len <=0 || pos <0 || offset <0 || nlen <= 0);
	}

	public HashLocPair() {

	}

	public HashLocPair(byte[] b) throws IOException {
		ByteBuffer bf = ByteBuffer.wrap(b);
		hash = new byte[HashFunctionPool.hashLength];
		hashloc = new byte[8];
		bf.get(hash);
		bf.get(hashloc);
		len = bf.getInt();
		pos = bf.getInt();
		offset = bf.getInt();
		nlen = bf.getInt();
		this.checkCorrupt();
	}

	@Override
	public int compareTo(HashLocPair p) {
		if (this.pos == p.pos)
			return 0;
		if (this.pos > p.pos)
			return 1;
		else
			return -1;
	}

	public HashLocPair clone() {
		HashLocPair p = new HashLocPair();
		p.hash = Arrays.copyOf(this.hash, this.hash.length);
		p.hashloc = Arrays.copyOf(this.hashloc, this.hashloc.length);
		p.len = len;
		p.pos = pos;
		p.offset = offset;
		p.nlen = nlen;
		return p;
	}
	
	public String toString() {
		return "pos=" +pos + " len=" + len + " offset=" + offset + " nlen=" + nlen + " ep=" + (pos + nlen);
	}

}