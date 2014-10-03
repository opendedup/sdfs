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

import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.sdfs.Main;

public class SparseDataChunk implements Externalizable {
	private ReentrantLock l = new ReentrantLock();
	private int doop;
	private int prevdoop;
	private boolean localData = false;
	// private int RAWDL;
	private long fpos;
	private byte version = 2;
	private static final long serialVersionUID = -2782607786999940224L;
	public int len = 0;
	private List<HashLocPair> ar = new ArrayList<HashLocPair>();

	public SparseDataChunk() {

	}

	public SparseDataChunk(byte[] rawData, int version) throws IOException {

		ByteBuffer buf = ByteBuffer.wrap(rawData);

		buf.get();
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

	public SparseDataChunk(int doop, List<HashLocPair> ar, boolean localData,
			byte version) {
		this.version = version;
		this.localData = localData;
		this.doop = doop;
		this.ar = ar;

	}

	public int getDoop() {
		return doop;
	}

	public HashLocPair getWL(int _pos) {
		l.lock();
		HashLocPair _h = null;
		HashLocPair h = null;
		try {

			for (int i = 0; i < ar.size(); i++) {

				h = ar.get(i);
				if (ar.size() == (i + 1)) {
					_h = h.clone();
					int os = _pos - _h.pos;
					_h.offset += os;
					_h.nlen -= os;
					return _h;
				}
				HashLocPair nh = ar.get(i + 1);
				if (_pos >= h.pos && _pos < nh.pos) {
					_h = h.clone();
					int os = _pos - _h.pos;
					_h.offset += os;
					_h.nlen -= os;
					return _h;
				}
			}
		} finally {
			l.unlock();
		}
		return _h;

	}

	public void putHash(HashLocPair p) throws IOException {
		l.lock();
		try {
			int ep = p.pos + p.nlen;
			if (ep > Main.CHUNK_LENGTH)
				throw new IOException("Overflow ep=" + ep);
			ArrayList<HashLocPair> rm = null;
			ArrayList<HashLocPair> am = null;
			// SDFSLogger.getLog().info("p = " + p);

			for (HashLocPair h : ar) {
				int hep = h.pos + h.nlen;
					if (h.pos >= p.pos && hep <= ep) {
						// SDFSLogger.getLog().info("0 removing h = " + h);
						if (rm == null)
							rm = new ArrayList<HashLocPair>();
						rm.add(h);
					} else if (h.pos <= p.pos && hep > p.pos) {
						if(hep > ep) {
							int offset = ep - h.pos;
							HashLocPair _h = h.clone();
							_h.offset += offset;
							_h.nlen -=offset;
							_h.pos = ep;
							_h.hashloc[0]=1;
							if (am == null)
								am = new ArrayList<HashLocPair>();
							am.add(_h);
						}
						if(h.pos < p.pos)
							h.nlen = (p.pos - h.pos);
						else {
							if (rm == null)
								rm = new ArrayList<HashLocPair>();
							rm.add(h);
						}
					} else if (h.pos > p.pos && h.pos <= ep) {
						int no = ep - h.pos;
						// int oh = h.pos;
						h.pos = (p.pos + p.nlen);
						h.offset += no;
						h.nlen -= no;
						// SDFSLogger.getLog().info("2 changing pos  from " +oh
						// +" to " + h.pos + " offset = " + h.offset);
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
			p.hashloc[0]=1;
			ar.add(p);
			Collections.sort(this.ar);
		} finally {
			l.unlock();
		}
	}

	public byte[] getBytes() throws IOException {
		l.lock();
		try {
			ByteBuffer buf = null;
			buf = ByteBuffer.wrap(new byte[1 + 4 + 4 + 4
					+ (ar.size() * HashLocPair.BAL)]);
			this.prevdoop = this.doop;
			this.doop = 0;
			buf.put(this.version);
			buf.putInt(buf.capacity());
			buf.putInt(this.ar.size());
			Collections.sort(this.ar);
			if(ar.size() > (HashFunctionPool.max_hash_cluster*2))
				throw new IOException("Buffer overflow ar size = " + ar.size() + " max size = " +(HashFunctionPool.max_hash_cluster*2));
			this.len = 0;
			for (HashLocPair p : ar) {
				if(p.hashloc[0] ==1)
					this.doop += p.nlen;
				buf.put(p.asArray());
				this.len += p.nlen;
			}
			buf.putInt(this.doop);
			return buf.array();
		} finally {
			l.unlock();
		}
	}

	public boolean isLocalData() {
		return localData;
	}

	public void setLocalData(boolean local) {
		this.localData = local;
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
	public void readExternal(ObjectInput arg0) throws IOException,
			ClassNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void writeExternal(ObjectOutput arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	public int getPrevdoop() {
		return prevdoop;
	}

}
