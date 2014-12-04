package org.opendedup.sdfs.io;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.bouncycastle.util.Arrays;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;

public class SparseDataChunk implements Externalizable {
	private ReentrantLock l = new ReentrantLock();
	private int doop;
	private int prevdoop;
	// private int RAWDL;
	private long fpos;
	private static final long serialVersionUID = -2782607786999940224L;
	public int len = 0;
	public byte flags = 0;
	public static final int RECONSTRUCTED = 1; // 0001
	private byte version = 0;
	private List<HashLocPair> ar = new ArrayList<HashLocPair>();
	private static final byte[] hlf = new byte[HashFunctionPool.hashLength];

	public SparseDataChunk() {

	}

	public SparseDataChunk(byte[] rawData, byte version) throws IOException {
		this.version = version;
		this.marshall(rawData);
	}

	public SparseDataChunk(int doop, List<HashLocPair> ar, boolean localData,
			byte version) {
		this.version = version;
		this.doop = doop;
		this.ar = ar;

	}

	private void marshall(byte[] raw) throws IOException {
		ByteBuffer buf = ByteBuffer.wrap(raw);
		if (this.version == 0) {
			ar = new ArrayList<HashLocPair>(1);
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
			ar.add(p);
		} else if (version == 1) {
			this.doop = buf.getInt();
			ar = new ArrayList<HashLocPair>();
			for (int i = 0; i < HashFunctionPool.max_hash_cluster; i++) {
				byte[] hash = new byte[HashFunctionPool.hashLength];
				buf.get(hash);
				if (!Arrays.areEqual(hash, hlf)) {
					HashLocPair p = new HashLocPair();
					p.hash = hash;
					p.pos = -1;
					ar.add(p);
				}
			}
			for (HashLocPair p : ar) {
				byte[] b = new byte[8];
				buf.get(b);
				p.hashloc = b;
			}

		} else {

			this.flags = buf.get();
			buf.getInt();
			int zlen = buf.getInt();
			ar = new ArrayList<HashLocPair>(zlen);
			for (int i = 0; i < zlen; i++) {
				byte[] b = new byte[HashLocPair.BAL];
				buf.get(b);
				HashLocPair p = new HashLocPair(b);
				ar.add(p);
				int ep = p.pos + p.len;
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
		l.lock();
		try {
			for (HashLocPair h : ar) {
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
			for (HashLocPair h : ar) {
				SDFSLogger.getLog().info(h);
			}
			throw new IOException("Position not found " + _pos);
		} finally {
			l.unlock();
		}

	}

	public static void insertHashLocPair(List<HashLocPair> ar, HashLocPair p)
			throws IOException {
		int ep = p.pos + p.nlen;
		if (ep > Main.CHUNK_LENGTH)
			throw new IOException("Overflow ep=" + ep);
		ArrayList<HashLocPair> rm = null;
		ArrayList<HashLocPair> am = null;
		// SDFSLogger.getLog().info("p = " + p);

		for (HashLocPair h : ar) {
			int hep = h.pos + h.nlen;
			if (h.pos >= ep)
				break;
			else if (h.pos >= p.pos && hep <= ep) {
				// SDFSLogger.getLog().info("0 removing h = " + h);
				if (rm == null)
					rm = new ArrayList<HashLocPair>();
				rm.add(h);
			} else if (h.pos >= p.pos && h.pos < ep && hep > ep) {
				int no = ep - h.pos;
				// int oh = h.pos;
				h.pos = ep;
				h.offset += no;
				h.nlen -= no;

				// SDFSLogger.getLog().info("2 changing pos  from " +oh
				// +" to " + h.pos + " offset = " + h.offset);
			} else if (h.pos <= p.pos && hep > p.pos) {
				if (hep > ep) {
					int offset = ep - h.pos;
					HashLocPair _h = h.clone();
					_h.offset += offset;
					_h.nlen -= offset;
					_h.pos = ep;
					_h.hashloc[0] = 1;
					if (am == null)
						am = new ArrayList<HashLocPair>();

					am.add(_h);
				}
				if (h.pos < p.pos) {
					h.nlen = (p.pos - h.pos);
				} else {
					SDFSLogger.getLog().info("should not get here");
					SDFSLogger.getLog().info(p);
					SDFSLogger.getLog().info(h);
					if (rm == null)
						rm = new ArrayList<HashLocPair>();
					rm.add(h);
				}
			}
			if (h.isInvalid()) {
				SDFSLogger.getLog().error("h = " + h.toString());
			}
		}
		if (rm != null) {
			for (HashLocPair z : rm) {
				ar.remove(z);
			}
		}
		if (am != null) {
			for (HashLocPair z : am) {
				ar.add(z);
			}
		}
		p.hashloc[0] = 1;
		ar.add(p);

		Collections.sort(ar);
	}

	public void putHash(HashLocPair p) throws IOException {
		l.lock();
		try {
			insertHashLocPair(ar, p);
			this.flags = RECONSTRUCTED;
		} finally {
			l.unlock();
		}
	}

	public void setRecontructed(boolean reconstructed) {
		if (reconstructed)
			this.flags = RECONSTRUCTED;

	}

	public byte[] getBytes() throws IOException {
		l.lock();
		try {
			if (this.version == 0) {
				ByteBuffer buf = ByteBuffer
						.wrap(new byte[LongByteArrayMap._FREE.length]);
				if (doop > 0)
					buf.put((byte) 1);
				else
					buf.put((byte) 0);
				buf.put(ar.get(0).hash);
				buf.put((byte) 0);
				buf.put(ar.get(0).hashloc);
				return buf.array();
			} else if (this.version == 1) {
				ByteBuffer buf = ByteBuffer
						.wrap(new byte[LongByteArrayMap._v1arrayLength]);
				buf.putInt(doop);
				for (HashLocPair p : ar) {
					buf.put(p.hash);
				}
				for (HashLocPair p : ar) {
					buf.put(p.hashloc);
				}
				return buf.array();

			} else {
				ByteBuffer buf = null;
				buf = ByteBuffer.wrap(new byte[1 + 4 + 4 + 4
						+ (ar.size() * HashLocPair.BAL)]);
				this.prevdoop = this.doop;
				this.doop = 0;
				buf.put(this.flags);
				buf.putInt(buf.capacity());
				buf.putInt(this.ar.size());
				Collections.sort(this.ar);
				if (ar.size() > (LongByteArrayMap.MAX_ELEMENTS_PER_AR)) {
					SDFSLogger.getLog().error(
							"Buffer overflow ar size = " + ar.size()
									+ " max size = "
									+ (LongByteArrayMap.MAX_ELEMENTS_PER_AR));
					throw new IOException("Buffer overflow ar size = "
							+ ar.size() + " max size = "
							+ (LongByteArrayMap.MAX_ELEMENTS_PER_AR));
				}
				this.len = 0;
				for (HashLocPair p : ar) {
					if (p.hashloc[0] == 1)
						this.doop += p.nlen;
					buf.put(p.asArray());
					this.len += p.nlen;
				}
				buf.putInt(this.doop);
				return buf.array();
			}
		} finally {
			l.unlock();
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

	public List<HashLocPair> getFingers() {
		return ar;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
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
