/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.sdfs.io;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.rabin.utils.StringUtils;

import com.google.common.collect.Range;

public class HashLocPair implements Comparable<HashLocPair>, Externalizable {
	public static final int BAL = HashFunctionPool.hashLength + 8 + 4 + 4 + 4
			+ 4;
	public byte[] hash;
	public byte[] hashloc;
	public byte[] data;
	public int len;
	public int pos;
	public int offset;
	public int nlen;
	private boolean dup = false;
	public boolean inserted = false;

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

	private void checkCorrupt() throws IOException {
		if (len < 0 || pos < 0 || offset < 0 || nlen < 0)
			throw new IOException("data is corrupt " + this);
	}

	public boolean isInvalid() {
		return (len <= 0 || pos < 0 || offset < 0 || nlen <= 0);
	}

	public HashLocPair() {

	}

	private int currentPos = 1;

	public synchronized void addHashLoc(byte loc) {
		// SDFSLogger.getLog().info("set " + this.currentPos + " to " + loc);
		if (currentPos < this.hashloc.length) {
			if (this.hashloc[0] == -1)
				this.hashloc[0] = 0;
			this.hashloc[currentPos] = loc;
			this.currentPos++;
		}
	}

	public void resetHashLoc() {
		this.hashloc = new byte[8];
		this.hashloc[0] = -1;
		currentPos = 1;
	}

	public int getNumberHL() {
		return this.currentPos - 1;
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

	public Range<Integer> getRange() {
		return Range.closed(pos, pos + nlen);
	}

	public String toString() {
		String hashlocs = "[";
		for (byte b : this.hashloc) {
			hashlocs = hashlocs + Byte.toString(b) + " ";
		}
		hashlocs = hashlocs + "]";
		return "pos=" + pos + " len=" + len + " offset=" + offset + " nlen="
				+ nlen + " ep=" + (pos + nlen) + " hash="
				+ StringUtils.getHexString(hash) + " hashlocs=" + hashlocs;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		in.readInt();
		this.hash = new byte[in.readInt()];
		in.read(this.hash);
		this.hashloc = new byte[8];
		in.read(this.hashloc);

	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		ByteBuffer bf = ByteBuffer.wrap(new byte[4 + this.hash.length
				+ this.hashloc.length]);
		bf.putInt(this.hash.length);
		bf.put(hash);
		bf.put(hashloc);
		byte[] b = bf.array();
		out.writeInt(b.length);
		out.write(b);

	}

	public boolean isDup() {
		return dup;
	}

	public void setDup(boolean dup) {
		this.dup = dup;
	}
	
	@Override
	public boolean equals(Object obj) {
		HashLocPair p = (HashLocPair)obj;
		if(Arrays.equals(p.hash, this.hash) && Arrays.equals(p.hashloc, this.hashloc))
			return true;
		else
			return false;
	}

	
	@Override
	public int hashCode() {
		return ByteBuffer.wrap(this.hash).getInt();
	}
}