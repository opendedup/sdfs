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

import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;


public class SparseDataChunk implements Externalizable {
	private ReentrantLock l = new ReentrantLock();
	private int doop;
	private int prevdoop;
	private boolean localData = false;
	// private int RAWDL;
	private long fpos;
	private static final long serialVersionUID = -2782607786999940224L;
	public int len = 0;
	public byte flags = 0;
	public static final int RECONSTRUCTED = 1; // 0001
	private List<HashLocPair> ar = new ArrayList<HashLocPair>();

	public SparseDataChunk() {

	}

	public SparseDataChunk(byte[] rawData, byte version) throws IOException {
		this.marshall(rawData);
	}

	public SparseDataChunk(int doop, List<HashLocPair> ar, boolean localData,
			byte version) {
		this.localData = localData;
		this.doop = doop;
		this.ar = ar;

	}

	private void marshall(byte[] raw) throws IOException {
		ByteBuffer buf = ByteBuffer.wrap(raw);

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

	public int getDoop() {
		return doop;
	}
	
	

	public HashLocPair getWL(int _pos) {
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
				else if(h.pos > _pos) {
					HashLocPair _h = new HashLocPair();
					_h.pos = _pos;
					_h.nlen = h.pos - _pos;
					_h.np = true;
					return _h;
				}
			}
			HashLocPair _h =null;
			_h = new HashLocPair();
			_h.pos = _pos;
			_h.nlen = Main.CHUNK_LENGTH - _pos;
			_h.np = true;
			return _h;
		} finally {
			l.unlock();
		}
		

	}
	
	public static void insertHashLocPair(List<HashLocPair> ar,HashLocPair p) throws IOException {
			int ep = p.pos + p.nlen;
			if (ep > Main.CHUNK_LENGTH)
				throw new IOException("Overflow ep=" + ep);
			ArrayList<HashLocPair> rm = null;
			ArrayList<HashLocPair> am = null;
			// SDFSLogger.getLog().info("p = " + p);

			for (HashLocPair h : ar) {
				int hep = h.pos + h.nlen;
				if(h.pos >= ep)
					break;
				else if (h.pos >= p.pos && hep <= ep) {
					// SDFSLogger.getLog().info("0 removing h = " + h);
					if (rm == null)
						rm = new ArrayList<HashLocPair>();
					rm.add(h);
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
					if (h.pos < p.pos)
						h.nlen = (p.pos - h.pos);
					else {
						if (rm == null)
							rm = new ArrayList<HashLocPair>();
						rm.add(h);
					}
				} else if (h.pos >= p.pos && h.pos <= ep && hep > ep) {
					int no = ep - h.pos;
					// int oh = h.pos;
					h.pos = ep;
					h.offset += no;
					h.nlen -= no;
					
					// SDFSLogger.getLog().info("2 changing pos  from " +oh
					// +" to " + h.pos + " offset = " + h.offset);
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
			insertHashLocPair(ar,p);
			this.flags = RECONSTRUCTED;
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
			buf.put(this.flags);
			buf.putInt(buf.capacity());
			buf.putInt(this.ar.size());
			Collections.sort(this.ar);
			if (ar.size() > (LongByteArrayMap.MAX_ELEMENTS_PER_AR)) {
				SDFSLogger.getLog().error(
						"Buffer overflow ar size = " + ar.size()
								+ " max size = "
								+ (LongByteArrayMap.MAX_ELEMENTS_PER_AR));
				throw new IOException("Buffer overflow ar size = " + ar.size()
						+ " max size = "
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
