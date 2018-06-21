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
package org.opendedup.collections;

import java.io.Externalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.HashLocPair;
//import org.opendedup.util.StringUtils;
import org.opendedup.util.StringUtils;

public class SparseDataChunk implements Externalizable {
	private ReentrantReadWriteLock l = new ReentrantReadWriteLock();
	private int doop;
	private int prevdoop;
	// private int RAWDL;
	private long fpos;
	private static final long serialVersionUID = -2782607786999940224L;
	public int len = 0;
	public byte flags = 0;
	public static final int RECONSTRUCTED = 1; // 0001
	private byte version = 0;
	private TreeMap<Integer, HashLocPair> ar = new TreeMap<Integer, HashLocPair>();

	public SparseDataChunk() {

	}

	public SparseDataChunk(byte[] rawData, byte version) throws IOException {
		this.version = version;
		this.marshall(rawData);
	}

	public SparseDataChunk(int doop, TreeMap<Integer, HashLocPair> ar, boolean localData, byte version) {

		this.version = version;
		this.doop = doop;
		for (HashLocPair p : ar.values()) {
			this.ar.put(p.pos, p);
		}

	}
	
	public byte getVersion() {
		return this.version;
	}

	private void marshall(byte[] raw) throws IOException {
		ByteBuffer buf = ByteBuffer.wrap(raw);
		ar = new TreeMap<Integer, HashLocPair>();
		if (this.version == 0) {

			byte b = buf.get();
			if (b == 0)
				doop = 0;
			else
				doop = Main.CHUNK_LENGTH;
			HashLocPair p = new HashLocPair();
			p.hash = new byte[HashFunctionPool.hashLength];
			buf.get(p.hash);
			buf.get();
			p.hashloc = new byte[8];
			buf.get(p.hashloc);
			p.pos = 0;
			p.len = Main.CHUNK_LENGTH;
			p.nlen = p.len;
			p.offset = 0;
			ar.put(p.pos, p);
		} else if (version == 1) {
			this.doop = buf.getInt();
			ar = new TreeMap<Integer, HashLocPair>();
			byte[] hash = new byte[HashFunctionPool.hashLength * HashFunctionPool.max_hash_cluster];
			buf.get(hash);
			byte[] hashlocs = new byte[8 * HashFunctionPool.max_hash_cluster];
			buf.get(hashlocs);
			ByteBuffer hb = ByteBuffer.wrap(hash);
			ByteBuffer hl = ByteBuffer.wrap(hashlocs);
			for (int z = 0; z < HashFunctionPool.max_hash_cluster; z++) {
				byte[] _hash = new byte[HashFunctionPool.hashLength];
				byte[] _hl = new byte[8];
				hl.get(_hl);

				hb.get(_hash);
				if (_hl[1] != 0) {
					HashLocPair p = new HashLocPair();
					p.hash = _hash;
					p.hashloc = _hl;
					p.pos = -1;
					ar.put(z, p);
				} else
					break;
			}

		} else {
			this.flags = buf.get();
			buf.getInt();
			int zlen = buf.getInt();
			ar = new TreeMap<Integer, HashLocPair>();
			for (int i = 0; i < zlen; i++) {
				byte[] b = new byte[HashLocPair.BAL];
				buf.get(b);
				HashLocPair p = new HashLocPair(b);
				ar.put(p.pos, p);
				int ep = p.pos + p.nlen;
				if (ep > len)
					len = ep;
			}
			doop = buf.getInt();
		}
	}

	public int getDoop() {
		return doop;
	}

	public HashLocPair getWL(int _pos) throws IOException {
		l.readLock().lock();
		try {

			Entry<Integer, HashLocPair> he = this.ar.floorEntry(_pos);
			if (he != null) {
				HashLocPair h = he.getValue();
				int ep = h.pos + h.nlen;
				if (_pos >= h.pos && _pos < ep) {
					HashLocPair _h = h.clone();
					int os = _pos - _h.pos;
					_h.offset += os;
					_h.nlen -= os;
					_h.pos = _pos;
					return _h;
				}
			}
			for (HashLocPair h : ar.values()) {
				SDFSLogger.getLog().warn("Pos  = " + _pos + " not found in =" + h);
			}
			throw new IOException("Position not found " + _pos);
		} finally {
			l.readLock().unlock();
		}

	}

	public static void insertHashLocPair(TreeMap<Integer, HashLocPair> ar, HashLocPair p, String lookupFilter)
			throws IOException {
		int ep = p.pos + p.nlen;
		if (ep > Main.CHUNK_LENGTH)
			throw new IOException("Overflow ep=" + ep + " sp=" + p.pos);
		// SDFSLogger.getLog().info("p = " + p);
		int _ep = ep;
		int k = 0;
		
		for (;;) {
			Entry<Integer, HashLocPair> he = ar.floorEntry(_ep);
			if (he == null)
				break;
			HashLocPair h = he.getValue();
			int hpos = h.pos;
			int hep = h.pos + h.nlen;
			SDFSLogger.getLog().debug("cheching k="+k+ " floor=" + _ep+ " p.pos=" + p.pos
			+ " h.pos="+ h.pos + " p.ep="+ep+ " h.ep="+hep+" p.hash=" +
			StringUtils.getHexString(p.hash) +" h.hash=" +
			StringUtils.getHexString(h.hash));
			if (hep < p.pos) {
				break;
			}
			if (h.pos >= p.pos) {
				int no = ep - h.pos;
				// int oh = h.pos;
				h.pos = ep;
				h.offset += no;
				h.nlen -= no;

				ar.remove(hpos);
				if (h.nlen > 0) {
					ar.put(h.pos, h);
				}
				// SDFSLogger.getLog().info("2 changing pos from " +oh
				// +" to " + h.pos + " offset = " + h.offset);
			} else if (h.pos < p.pos && hep >= p.pos) {
				if (hep > ep) {
					int offset = ep - h.pos;
					HashLocPair _h = h.clone();
					_h.offset += offset;
					_h.nlen -= offset;
					_h.pos = ep;
						_h.setDup(true);
					if(_h.nlen <= 0)
						SDFSLogger.getLog().error("LZ " + _h);
					ar.put(_h.pos, h);
				}
				if (h.pos < p.pos) {
					h.nlen = (p.pos - h.pos);
				} else {
					ar.remove(h.pos);
				}
				if(h.nlen <=0)
					ar.remove(h.pos);
			}
			_ep = h.pos - 1;
			k++;
		}
		ar.put(p.pos, p);

	}

	public void setRecontructed(boolean reconstructed) {
		if (reconstructed)
			this.flags = RECONSTRUCTED;

	}

	public byte[] getBytes() throws IOException {
		l.readLock().lock();
		try {
			if (this.version == 0) {
				ByteBuffer buf = ByteBuffer.wrap(new byte[LongByteArrayMap._FREE.length]);
				if (doop > 0)
					buf.put((byte) 1);
				else
					buf.put((byte) 0);
				buf.put(ar.get(0).hash);
				buf.put((byte) 0);
				buf.put(ar.get(0).hashloc);
				return buf.array();
			} else if (this.version == 1) {
				ByteBuffer buf = ByteBuffer.wrap(new byte[LongByteArrayMap._v1arrayLength]);
				buf.putInt(doop);
				for (HashLocPair p : ar.values()) {
					buf.put(p.hash);
				}
				for (HashLocPair p : ar.values()) {
					buf.put(p.hashloc);
				}
				return buf.array();

			} else {
				ByteBuffer buf = null;
				buf = ByteBuffer.wrap(new byte[1 + 4 + 4 + 4 + (ar.size() * HashLocPair.BAL)]);
				this.prevdoop = this.doop;
				this.doop = 0;
				buf.put(this.flags);
				buf.putInt(buf.capacity());
				buf.putInt(this.ar.size());
				if (ar.size() > (LongByteArrayMap.MAX_ELEMENTS_PER_AR)) {
					SDFSLogger.getLog().error("Buffer overflow ar size = " + ar.size() + " max size = "
							+ (LongByteArrayMap.MAX_ELEMENTS_PER_AR));
					throw new IOException("Buffer overflow ar size = " + ar.size() + " max size = "
							+ (LongByteArrayMap.MAX_ELEMENTS_PER_AR));
				}
				this.len = 0;
				for (HashLocPair p : ar.values()) {
					boolean dup = p.isDup();
					if (dup)
						this.doop += p.nlen;
					buf.put(p.asArray());
					this.len += p.nlen;
				}
				buf.putInt(this.doop);
				return buf.array();
			}
		} finally {
			l.readLock().unlock();
		}
	}

	public void setDoop(int doop) {
		this.doop = doop;
	}

	public long getFpos() {
		return fpos;
	}

	public void setFpos(long fpos) {
		this.fpos = fpos;
	}

	public TreeMap<Integer, HashLocPair> getFingers() {
		return ar;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		byte[] b = new byte[in.readInt()];
		this.marshall(b);

	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		byte[] b = this.getBytes();
		out.writeInt(b.length);
		out.write(b);

	}

	public int getPrevdoop() {
		return prevdoop;
	}

	public boolean isRecontructed() {
		if (this.flags == 0)
			return false;
		else
			return true;
	}

}