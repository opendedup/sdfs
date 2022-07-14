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

import java.io.IOException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.collections.DataArchivedException;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.SparseDataChunk;
import org.opendedup.hashing.AbstractHashEngine;
import org.opendedup.hashing.Finger;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.StringUtils;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Longs;

/**
 *
 * @author annesam WritableCacheBuffer is used to store written data for later
 *         writing and reading by the DedupFile. WritableCacheBuffers are
 *         evicted from the file system based LRU. When a writable cache buffer
 *         is evicted it is then written to the dedup chunk service
 */
public class WritableCacheBuffer implements DedupChunkInterface, Runnable {

	private ByteBuffer buf = null;
	private byte[] bar = null;
	private boolean dirty = false;

	private long endPosition = 0;
	// private int currentLen = 0;
	private int length;
	private final long position;
	private boolean newChunk = false;
	private boolean writable = false;
	private int doop = 0;
	private int bytesWritten = 0;
	private SparseDedupFile df;
	protected boolean closed = true;
	protected boolean flushing = false;
	boolean rafInit = false;
	int prevDoop = 0;
	private boolean batchprocessed;
	private boolean batchwritten;
	private boolean reconstructed;
	private boolean hlAdded = false;
	private boolean direct = false;
	protected ReentrantLock lobj = new ReentrantLock();
	private TreeMap<Integer, HashLocPair> ar = new TreeMap<Integer, HashLocPair>();
	private TreeMap<Integer, HashLocPair> _ar = null;
	int sz;
	private static SynchronousQueue<Runnable> lworksQueue = null;
	private static ThreadPoolExecutor lexecutor = null;
	private static ThreadPoolExecutor executor = null;
	private static SynchronousQueue<Runnable> worksQueue = null;
	public static final byte[] blankBlock = new byte[HashFunctionPool.minLen + 1];
	public static final byte[] bk = SparseDedupFile.eng.getHash(blankBlock);
	// private static AtomicLong wbsz= new AtomicLong();
	static {
		SDFSLogger.getLog().info("blankHash=" + StringUtils.getHexString(bk));

		worksQueue = new SynchronousQueue<Runnable>();
		executor = new ThreadPoolExecutor(Main.writeThreads, Main.writeThreads, 0L, TimeUnit.SECONDS, worksQueue,
				new ThreadPoolExecutor.CallerRunsPolicy());
		lworksQueue = new SynchronousQueue<Runnable>();
		lexecutor = new ThreadPoolExecutor(Main.writeThreads, Main.writeThreads, 0L, TimeUnit.SECONDS, lworksQueue,
				new ThreadPoolExecutor.CallerRunsPolicy());
	}

	public WritableCacheBuffer(long startPos, int length, SparseDedupFile df, TreeMap<Integer, HashLocPair> ar,
			boolean reconstructed) throws IOException {
		this.length = length;
		this.position = startPos;
		this.newChunk = true;
		this.ar = ar;
		this.df = df;
		this.reconstructed = reconstructed;
		bar = new byte[Main.CHUNK_LENGTH];
		buf = ByteBuffer.wrap(bar);
		if (this.df.bdb.getVersion() >= 2) {
			this.direct = true;
		}
		// this.currentLen = 0;
		this.setLength(Main.CHUNK_LENGTH);
		this.endPosition = this.getFilePosition() + this.getLength();
		this.setWritable(true);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#getBytesWritten()
	 */
	@Override
	public int getBytesWritten() {
		return bytesWritten;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#getDedupFile()
	 */
	@Override
	public DedupFile getDedupFile() {
		return this.df;
	}

	public WritableCacheBuffer(DedupChunkInterface dk, DedupFile df) throws IOException {
		this.position = dk.getFilePosition();
		this.length = dk.getLength();
		this.newChunk = dk.isNewChunk();
		this.prevDoop = dk.getPrevDoop();
		this.reconstructed = dk.getReconstructed();
		this.ar = dk.getFingers();
		this.df = (SparseDedupFile) df;
		this.setLength(Main.CHUNK_LENGTH);
		this.endPosition = this.getFilePosition() + this.getLength();
		this.setWritable(true);

		if (this.df.bdb.getVersion() >= 2) {
			this.direct = true;
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#sync()
	 */
	@Override
	public boolean sync() throws IOException {

		return false;
	}

	public byte[] getReadChunk(int startPos, int len) throws IOException, BufferClosedException, DataArchivedException {

		SDFSLogger.getLog().debug("reading " + df.getMetaFile().getPath() + " df=" + df.getGUID() + " fpos="
				+ this.position + " start=" + startPos + " len=" + len);
		lobj.lock();
		try {
			if (this.closed)
				throw new BufferClosedException("Buffer Closed");
			if (this.flushing)
				throw new BufferClosedException("Buffer Flushing");
			if (this.hlAdded) {
				ByteBuffer bf = ByteBuffer.wrap(new byte[len]);
				int ep = startPos + len;
				Integer k = this.ar.floorKey(startPos);
				if (k == null) {
					return this.readChunk(startPos, len);
				} else {
					while (k < ep) {
						HashLocPair s = this.ar.get(k);
						byte[] b = null;
						if (Arrays.equals(s.hash, bk))
							b = blankBlock;
						else
							b = HCServiceProxy.fetchChunk(s.hash, s.hashloc, direct);
						if (bf.remaining() < b.length) {
							bf.put(b, 0, bf.remaining());
							break;
						} else
							bf.put(b);
						k = this.ar.floorKey(k + b.length + 1);
					}
				}
				return bf.array();
			} else {
				return this.readChunk(startPos, len);
			}

		} finally {
			lobj.unlock();
		}

	}

	private byte[] readChunk(int startPos, int len) throws IOException, BufferClosedException, DataArchivedException {
		if (this.flushing) {
			SDFSLogger.getLog().debug("cannot read buffer at pos " + this.getFilePosition() + " already flushing");
			throw new BufferClosedException("Buffer Closed");

		}
		if (this.closed) {
			SDFSLogger.getLog().debug("cannot read buffer at pos " + this.getFilePosition() + " closed");
			throw new BufferClosedException("Buffer Closed");
		}
		if (this.accelBuffer != null) {
			if (startPos >= this.accelBuffPos && (startPos + len) <= this.accelBuffEp) {
				ByteBuffer _buf = ByteBuffer.wrap(this.accelBuffer.toByteArray());
				int sp = this.accelBuffPos - startPos;
				byte[] _b = new byte[len];
				_buf.position(sp);
				_buf.get(_b);
				return _b;
			}
		}
		try {
			this.initBuffer();
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
		byte[] dd = new byte[len];
		System.arraycopy(bar, startPos, dd, 0, len);
		return dd;
	}

	int tries = 0;

	private void initBuffer() throws IOException, InterruptedException, DataArchivedException {
		lobj.lock();
		try {
			this.writeAccelBuffer();
			while (this.buf == null) {
				this.hlAdded = false;
				if (HashFunctionPool.max_hash_cluster > 1) {
					this.bar = new byte[Main.CHUNK_LENGTH];
					this.buf = ByteBuffer.wrap(bar);
					final ArrayList<Shard> cks = new ArrayList<Shard>();
					int i = 0;
					// long fp = this.position;

					for (HashLocPair p : ar.values()) {

						if (Longs.fromByteArray(p.hashloc) != 0) {
							Shard sh = new Shard();
							sh.hash = p.hash;
							sh.hashloc = p.hashloc;
							sh.direct = this.direct;
							sh.pos = p.pos;
							sh.nlen = p.nlen;
							sh.offset = p.offset;
							sh.len = p.len;
							sh.apos = i;
							cks.add(i, sh);
						} else
							break;
						i++;
					}

					sz = cks.size();
					AsyncChunkReadActionListener l = new AsyncChunkReadActionListener() {

						@Override
						public void commandException(Exception e) {
							SDFSLogger.getLog().error("error getting block", e);
							this.incrementAndGetDNEX();
							synchronized (this) {
								this.notifyAll();
							}

						}

						@Override
						public void commandResponse(Shard result) {
							cks.get(result.apos).ck = result.ck;
							if (this.incrementandGetDN() >= sz) {

								synchronized (this) {
									this.notifyAll();
								}
							}
						}

						@Override
						public void commandArchiveException(DataArchivedException e) {
							this.incrementAndGetDNEX();
							this.setDAR(e);

							synchronized (this) {
								this.notifyAll();
							}

						}

					};
					for (Shard sh : cks) {
						sh.l = l;
						sh.direct = this.direct;
						executor.execute(sh);
					}
					int wl = 0;
					int tm = 1000;
					int al = 0;

					while (l.getDN() < sz && l.getDNEX() == 0) {
						if (al == 30) {
							int nt = wl / 1000;
							SDFSLogger.getLog()
									.debug("Slow io, waited [" + nt + "] seconds for all reads to complete.");
							al = 0;
						}
						if (Main.readTimeoutSeconds > 0 && wl > (Main.readTimeoutSeconds * tm)) {
							int nt = (tm * wl) / 1000;
							throw new IOException("read Timed Out after [" + nt + "] seconds. Expected [" + sz
									+ "] block read but only [" + l.getDN() + "] were completed");
						}
						if (l.getDAR() != null) {
							this.buf = null;
							throw l.getDAR();
						}
						if (l.getDNEX() > 0) {
							this.buf = null;
							throw new IOException("error while reading data");
						}
						synchronized (l) {
							l.wait(1000);
						}
						wl += 1000;
						al++;
					}
					if (l.getDAR() != null) {
						this.buf = null;
						throw l.getDAR();
					}
					if (l.getDNEX() > 0) {
						this.buf = null;
						throw new IOException("error while getting blocks " + l.getDNEX() + " errors found");

					}
					if (l.getDN() < sz) {
						this.buf = null;
						throw new IOException("thread timed out before read was complete ");
					}
					buf.position(0);
					for (Shard sh : cks) {
						if (sh.pos == -1) {
							try {
								buf.put(sh.ck);
							} catch (Exception e) {
								// SDFSLogger.getLog().info("pos = " + this.position
								// + "ck sz=" + sh.ck.length + " hcb sz=" +
								// hcb.position() + " cks sz=" +cks.size() + " len="
								// + (hcb.position() +sh.ck.length));
								this.buf = null;
								throw new IOException(e);
							}
						} else {
							try {
								buf.position(sh.pos);
								if (sh.nlen > sh.ck.length)
									buf.put(sh.ck, sh.offset, sh.ck.length);
								else
									buf.put(sh.ck, sh.offset, sh.nlen);

							} catch (Exception e) {

								String hp = StringUtils.getHexString(sh.hash);
								SDFSLogger.getLog()
										.error("hash=" + hp + " pos = " + this.position + " ck nlen=" + sh.nlen
												+ " ckoffset=" + sh.offset + " cklen=" + sh.ck.length + " hcbpos="
												+ buf.position() + " ckslen=" + sh.len + " len=" + (buf.capacity()));
								this.buf = null;
								throw new IOException(e);
							}
						}
					}

				} else {
					if (Arrays.equals(this.ar.get(0).hash, bk))
						this.buf = ByteBuffer.wrap(new byte[blankBlock.length]);
					else
						this.buf = ByteBuffer
								.wrap(HCServiceProxy.fetchChunk(this.ar.get(0).hash, this.ar.get(0).hashloc, direct));

				}
			}
		} finally {
			lobj.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#capacity()
	 */
	@Override
	public int capacity() {

		return Main.CHUNK_LENGTH;
	}

	public MetaDataDedupFile getMF() {
		return this.df.mf;
	}

	public boolean setAR(TreeMap<Integer, HashLocPair> al) {
		boolean allInserted = true;
		try {

			HashMap<HashLocPair, Integer> ct = new HashMap<HashLocPair, Integer>();
			for (Entry<Integer, HashLocPair> e : this.ar.entrySet()) {
				int val = -1;
				if (ct.containsKey(e.getValue())) {
					val = ct.get(e.getValue()) - 1;
				}
				ct.put(e.getValue(), val);
			}
			for (Entry<HashLocPair, Integer> e : ct.entrySet()) {
				long archiveId = Longs.fromByteArray(e.getKey().hashloc);
				long ci = DedupFileStore.addRef(e.getKey().hash, archiveId, e.getValue());
				if (ci != -1 && archiveId != ci)
					e.getKey().hashloc = Longs.toByteArray(ci);
				if (ci == -1) {
					allInserted = false;
				}
			}
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to remove reference", e);
		}
		if (allInserted) {
			this.ar = al;
		}
		return allInserted;

	}

	private void reReference() {
		SDFSLogger.getLog().debug("Rereferencing cache buffer");
		try {
			if (Main.refCount && this.ar != null && this.hlAdded) {
				HashMap<HashLocPair, Integer> ct = new HashMap<HashLocPair, Integer>();
				if (_ar != null) {
					for (Entry<Integer, HashLocPair> e : this._ar.entrySet()) {
						int val = -1;
						if (ct.containsKey(e.getValue())) {
							val = ct.get(e.getValue()) - 1;
						}
						ct.put(e.getValue(), val);
					}
					_ar = null;
				}
				for (Entry<Integer, HashLocPair> e : this.ar.entrySet()) {
					int val = 1;
					if (ct.containsKey(e.getValue())) {
						if (!e.getValue().inserted)
							val = ct.get(e.getValue()) + 1;
					}
					ct.put(e.getValue(), val);
				}

				for (Entry<HashLocPair, Integer> e : ct.entrySet()) {
					long archiveId = Longs.fromByteArray(e.getKey().hashloc);
					long ci = DedupFileStore.addRef(e.getKey().hash, archiveId, e.getValue());
					if (ci != -1 && ci != archiveId) {
						e.getKey().hashloc = Longs.toByteArray(ci);
					}
				}
			}
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to remove reference", e);
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#getEndPosition()
	 */
	@Override
	public long getEndPosition() {
		return endPosition;
	}

	private void writeBlock(byte[] b, int pos) throws IOException, DataArchivedException {
		try {
			this.initBuffer();
			System.arraycopy(b, 0, bar, pos, b.length);
			this.hlAdded = false;
			this.dirty = true;
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#write(byte[], int)
	 */

	@Override
	public void write(byte[] b, int pos) throws BufferClosedException, IOException, DataArchivedException {
		SDFSLogger.getLog().debug("writing " + df.getMetaFile().getPath() + "df=" + df.getGUID() + "fpos="
				+ this.position + " pos=" + pos + " len=" + b.length);
		/*
		 * if (df.getMetaFile().getPath().endsWith(".vmx") ||
		 * df.getMetaFile().getPath().endsWith(".vmx~")) {
		 * SDFSLogger.getLog()
		 * .debug("###### In wb Text of VMX=" + df.getMetaFile().getPath() + "=" + new
		 * String(b, "UTF-8"));
		 * }
		 */
		lobj.lock();
		try {
			if (this.closed)
				throw new BufferClosedException("Buffer Closed while writing");
			if (this.flushing)
				throw new BufferClosedException("Buffer Flushing");
			this.dirty = true;
			/*
			 * if(pos != 0) SDFSLogger.getLog().info("start at " + pos); if(b.length !=
			 * this.capacity()) SDFSLogger.getLog().info( "!capacity " + b.length);
			 */
			if (pos == 0 && b.length == Main.CHUNK_LENGTH) {
				this.buf = ByteBuffer.wrap(b);
			} else {

				if (this.buf == null && this.reconstructed && HashFunctionPool.max_hash_cluster > 1) {

					this.wm(b, pos);
				} else {
					// SDFSLogger.getLog().info("writing at " + pos + " recon="
					// + this.reconstructed + " sz=" + this.ar.size());
					this.writeBlock(b, pos);
				}
			}

			this.bytesWritten = this.bytesWritten + b.length;
		} finally {
			lobj.unlock();
		}
	}

	public void copyExtent(HashLocPair p) throws IOException, BufferClosedException, DataArchivedException {
		this.lobj.lock();
		try {
			if (this.closed)
				throw new BufferClosedException("Buffer Closed while writing");
			if (this.flushing)
				throw new BufferClosedException("Buffer Flushing");

			if (!this.dirty && this.buf != null) {
				this.buf = null;
			}
			if (_ar == null) {
				_ar = new TreeMap<Integer, HashLocPair>(ar);
			}
			if (this.buf != null || this.ar.size() >= LongByteArrayMap.MAX_ELEMENTS_PER_AR) {
				if (this.ar.size() >= LongByteArrayMap.MAX_ELEMENTS_PER_AR)
					SDFSLogger.getLog()
							.info("copy extent Chuck Array Size greater than " + LongByteArrayMap.MAX_ELEMENTS_PER_AR
									+ " at " + (this.getFilePosition() + p.pos) + " for file " + this.df.mf.getPath());
				byte[] b = null;
				if (Arrays.equals(p.hash, bk))
					b = new byte[blankBlock.length];
				else
					b = HCServiceProxy.fetchChunk(p.hash, p.hashloc, direct);
				ByteBuffer bf = ByteBuffer.wrap(b);
				byte[] z = new byte[p.nlen];
				bf.position(p.offset);
				bf.get(z);
				this.writeBlock(z, p.pos);
				this.reconstructed = false;
				this.hlAdded = false;
			} else {
				try {
					this.reconstructed = true;
					this.hlAdded = true;

					SparseDataChunk.insertHashLocPair(ar, p);
				} catch (Throwable e) {
					df.errOccured = true;
					SDFSLogger.getLog().error("Error inserting " + p, e);
					throw new IOException(e);
				}
			}
		} finally {
			this.lobj.unlock();
		}
	}

	ByteArrayDataOutput accelBuffer = null;
	int accelBuffPos = 0;
	int accelBuffEp = 0;

	private void wm(byte[] b, int pos) throws IOException {
		this.reconstructed = true;
		if (accelBuffer == null) {
			accelBuffPos = pos;
			accelBuffEp = accelBuffPos + b.length;
			accelBuffer = ByteStreams.newDataOutput();
			accelBuffer.write(b);
		} else if (accelBuffEp == pos) {
			accelBuffer.write(b);
			accelBuffEp = accelBuffPos + b.length;
		} else {
			writeAccelBuffer();
			accelBuffPos = pos;
			accelBuffEp = accelBuffPos + b.length;
			accelBuffer = ByteStreams.newDataOutput();
			accelBuffer.write(b);
		}
	}

	public void writeAccelBuffer() throws IOException {
		if (accelBuffer == null)
			return;
		int opos = accelBuffPos;
		AbstractHashEngine hc = (AbstractHashEngine) HashFunctionPool.borrowObject();
		byte[] b = accelBuffer.toByteArray();
		try {
			List<Finger> fs = hc.getChunks(b, this.df.getGUID());
			AsyncChunkWriteActionListener l = new AsyncChunkWriteActionListener() {

				@Override
				public void commandException(Finger result, Throwable e) {
					this.incrementAndGetDNEX();
					SDFSLogger.getLog().error("Error while getting hash", e);
					this.incrementandGetDN();

					synchronized (this) {
						this.notifyAll();
					}
				}

				@Override
				public void commandResponse(Finger result) {
					int _dn = this.incrementandGetDN();
					if (_dn >= this.getMaxSz()) {
						synchronized (this) {
							this.notifyAll();
						}
					}
				}

				@Override
				public void commandArchiveException(DataArchivedException e) {
					this.incrementAndGetDNEX();
					this.dar = e;
					SDFSLogger.getLog().error("Data has been archived", e);
					this.incrementandGetDN();

					synchronized (this) {
						this.notifyAll();
					}

				}

			};
			l.setMaxSize(fs.size());
			Finger.FingerPersister fp = new Finger.FingerPersister();
			fp.l = l;
			fp.fingers = fs;
			lexecutor.execute(fp);
			int wl = 0;
			int tm = 1000;

			int al = 0;
			while (l.getDN() < fs.size() && l.getDNEX() == 0) {
				if (al == 60) {
					int nt = wl / 1000;
					SDFSLogger.getLog().warn("Slow io, waited [" + nt + "] seconds for all writes to complete.");
					al = 0;
				}
				if (Main.writeTimeoutSeconds > 0 && wl > (Main.writeTimeoutSeconds * tm)) {
					int nt = (tm * wl) / 1000;
					df.toOccured = true;
					throw new IOException("Write Timed Out after [" + nt + "] seconds. Expected [" + fs.size()
							+ "] block writes but only [" + l.getDN() + "] were completed");
				}
				if (l.dar != null) {
					this.buf = null;
					throw l.dar;
				}
				if (l.getDNEX() > 0) {
					this.buf = null;
					throw new IOException("Unable to read shard");
				}
				synchronized (l) {
					l.wait(tm);
				}
				al++;
				wl += tm;
			}
			if (l.dar != null) {
				this.buf = null;
				throw l.dar;
			}
			if (l.getDN() < fs.size()) {
				this.buf = null;
				df.toOccured = true;
				throw new IOException("Write Timed Out expected [" + fs.size() + "] but got [" + l.getDN() + "]");
			}
			if (l.getDNEX() > 0) {
				this.buf = null;
				throw new IOException("Write Failed because unable to read shard");
			}
			int ep = opos;
			for (Finger f : fs) {
				HashLocPair p = new HashLocPair();
				try {
					p.hash = f.hash;
					p.hashloc = f.hl.getHashLocs();
					p.len = f.len;
					p.offset = 0;
					if ((ep + f.len) <= Main.CHUNK_LENGTH)
						p.nlen = f.len;
					else
						p.nlen = Main.CHUNK_LENGTH - ep;
					p.pos = ep;
					p.inserted = true;
					ep += f.len;
					int dups = 0;
					if (!f.hl.getInserted())
						dups = f.len;
					df.mf.getIOMonitor().addVirtualBytesWritten(f.len, true);
					if (dups > 0)
						df.mf.getIOMonitor().addActualBytesWritten(f.len, true);
					else
						df.mf.getIOMonitor().addDulicateData(dups, true);
					this.prevDoop += dups;
					SparseDataChunk.insertHashLocPair(ar, p);
				} catch (Throwable e) {
					SDFSLogger.getLog().warn("unable to write object finger pos=" + opos + " len=" + b.length, e);
					throw e;
					// SDFSLogger.getLog().info("this chunk size is "
					// + f.chunk.length);
				}
			}
			if (this.ar.size() >= LongByteArrayMap.MAX_ELEMENTS_PER_AR) {

				SDFSLogger.getLog()
						.info("write accelerator Chuck Array Size greater than " + LongByteArrayMap.MAX_ELEMENTS_PER_AR
								+ " at " + (this.getFilePosition() + accelBuffPos) + " for file "
								+ this.df.mf.getPath());
				this.initBuffer();
				this.dirty = true;

			} else {
				this.hlAdded = true;
			}
			accelBuffer = null;
		} catch (Throwable e) {
			df.errOccured = true;
			SDFSLogger.getLog().error("write failed", e);
			throw new IOException(e);
		} finally {
			HashFunctionPool.returnObject(hc);
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#truncate(int)
	 */
	@Override
	public void truncate(int len) throws BufferClosedException {
		lobj.lock();
		try {

			if (this.closed)
				throw new BufferClosedException("Buffer Closed");

			this.destroy();
			this.dirty = true;
		} finally {
			lobj.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#isDirty()
	 */
	@Override
	public boolean isDirty() {
		lobj.lock();
		try {
			return dirty;
		} finally {
			lobj.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#setDirty(boolean)
	 */
	@Override
	public void setDirty(boolean dirty) {
		lobj.lock();
		try {
			this.dirty = dirty;
		} finally {
			lobj.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#toString()
	 */
	@Override
	public String toString() {
		return this.hashCode() + ":" + this.getFilePosition() + ":" + this.getLength() + ":" + this.getEndPosition();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#open()
	 */
	@Override
	public void open() {
		// long ksz = wbsz.incrementAndGet();
		this.lobj.lock();
		try {
			this.df.removeBufferFromFlush(this);
			this.df.addOpenBuffer(this);
			this.closed = false;
			this.flushing = false;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Error while opening", e);
			throw new IllegalArgumentException("error");
		} finally {
			this.lobj.unlock();
		}

		// SDFSLogger.getLog().info(" wbsz=" + ksz + " ab=" + df.activeBuffers.size() +
		// " ob=" + df.openBuffers.size() + " fb=" + df.flushingBuffers.size() + " wb="
		// +SparseDedupFile.writeBuffers.size());
	}

	public void flush() throws BufferClosedException {
		lobj.lock();
		try {
			if (this.flushing) {
					SDFSLogger.getLog()
							.debug("cannot flush buffer at pos " + this.getFilePosition() + " already flushing");
				throw new BufferClosedException("Buffer Closed");

			}
			if (this.closed) {
				df.removeBufferFromFlush(this);
				df.removeOpenBuffer(this);
					SDFSLogger.getLog().debug("cannot flush buffer at pos " + this.getFilePosition() + " closed");
				throw new BufferClosedException("Buffer Closed");

			}
			this.flushing = true;
			if (this.dirty || this.isHlAdded()) {
				this.df.putBufferIntoFlush(this);
				lexecutor.execute(this);
			} else {
				// wbsz.decrementAndGet();
				df.removeOpenBuffer(this);
				this.flushing = false;
				this.closed = true;
			}
		} catch (Exception e) {
			SDFSLogger.getLog().debug("unable to flush", e);
		} finally {
			lobj.unlock();
		}

	}

	public boolean isClosed() {
		lobj.lock();
		try {
			return this.closed;
		} finally {
			lobj.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#close()
	 */
	@Override
	public void close() throws IOException {
		// long ksz = wbsz.decrementAndGet();
		lobj.lock();
		try {

			if (!this.flushing) {
				df.removeOpenBuffer(this);
				df.removeBufferFromFlush(this);
					SDFSLogger.getLog().debug("#### " + this.getFilePosition() + " not flushing ");
				return;
			}
			try {
				if (accelBuffer != null) {
					this.writeAccelBuffer();
				}
				if (this.closed) {

						SDFSLogger.getLog().debug(this.getFilePosition() + " already closed");
				} else if (this.dirty || this.hlAdded) {
					if (hlAdded) {
						this.reReference();
						this.df.updateExtents(this);
					} else {
						this.df.writeCache(this);
					}
					this.closed = true;
					this.flushing = false;
					this.dirty = false;
					this.hlAdded = false;
				} else {
					this.closed = true;
					this.flushing = false;
				}
				df.removeBufferFromFlush(this);
				df.removeOpenBuffer(this);

			} catch (Exception e) {
				SDFSLogger.getLog().warn("unable to close " + this.position, e);
				df.writeBuffers.put(this.getFilePosition(), this);
				this.open();
				SDFSLogger.getLog().warn("re-opened" + this.position);
				throw new IOException(e);
			} finally {
				try {

				} catch (Exception e) {
				}

			}
		} finally {
			if (lobj.isLocked())
				lobj.unlock();

			// SDFSLogger.getLog().info("close wbsz=" + ksz + " ab=" +
			// df.activeBuffers.size() + " ob=" + df.openBuffers.size() + " fb=" +
			// df.flushingBuffers.size() + " wb=" +SparseDedupFile.writeBuffers.size());
		}
	}

	public void startClose() {

		this.batchprocessed = true;
	}

	public boolean isBatchProcessed() {
		return this.batchprocessed;
	}

	public void endClose() throws IOException {
		try {
			if (!this.flushing) {
					SDFSLogger.getLog().debug("####" + this.getFilePosition() + " not flushing");
			} else if (this.closed) {
					SDFSLogger.getLog().debug(this.getFilePosition() + " already closed");
			} else {

				this.df.writeCache(this);
				df.removeBufferFromFlush(this);
				this.closed = true;
				this.flushing = false;
			}

		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			this.batchprocessed = false;
		}
	}

	public byte[] getFlushedBuffer()
			throws BufferClosedException, IOException, InterruptedException, DataArchivedException {
		if (this.closed) {
				SDFSLogger.getLog().debug(this.getFilePosition() + " already closed");
			throw new BufferClosedException("Buffer Closed");
		}
		if (!this.flushing) {
				SDFSLogger.getLog().debug(this.getFilePosition() + " not flushed");
			throw new BufferClosedException("Buffer not flushed");
		}
		if (this.buf == null)
			SDFSLogger.getLog().debug(this.getFilePosition() + " buffer is null");
		if (this.buf == null)
			this.initBuffer();
		return this.buf.array();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#persist()
	 */
	@Override
	public void persist() {
		lobj.lock();

		try {
			this.df.writeCache(this);
			this.closed = true;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Error while closing", e);
			throw new IllegalArgumentException("error while closing " + e.toString());
		} finally {
			lobj.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#destroy()
	 */
	@Override
	public void destroy() {

	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#isPrevDoop()
	 */
	@Override
	public int getPrevDoop() {
		return prevDoop;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#setPrevDoop(boolean)
	 */
	@Override
	public void setPrevDoop(int prevDoop) {
		this.prevDoop = prevDoop;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#hashCode()
	 */
	@Override
	public int hashCode() {
		lobj.lock();
		try {
			HashFunction hf = Hashing.murmur3_128(6442);
			try {
				this.initBuffer();
			} catch (IOException | InterruptedException | DataArchivedException e) {
				SDFSLogger.getLog().warn("error getting cache", e);
			}
			return hf.hashBytes(buf.array()).asInt();
		} finally {
			lobj.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#getLength()
	 */
	@Override
	public int getLength() {
		return this.length;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#getFilePosition()
	 */
	@Override
	public long getFilePosition() {
		return this.position;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#setLength(int)
	 */
	@Override
	public void setLength(int length) {
		this.length = length;

	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#isNewChunk()
	 */
	@Override
	public boolean isNewChunk() {
		return this.newChunk;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#setNewChunk(boolean)
	 */
	@Override
	public void setNewChunk(boolean newChunk) {
		this.newChunk = newChunk;

	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#setWritable(boolean)
	 */
	@Override
	public void setWritable(boolean writable) {
		this.writable = writable;

	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#isWritable()
	 */
	@Override
	public boolean isWritable() {
		return this.writable;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#setDoop(boolean)
	 */
	@Override
	public void setDoop(int doop) {
		this.doop = doop;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#isDoop()
	 */
	@Override
	public int getDoop() {
		return this.doop;
	}

	public boolean isBatchwritten() {
		return batchwritten;
	}

	public void setBatchwritten(boolean batchwritten) {
		this.batchwritten = batchwritten;
	}

	public static class BlockPolicy implements RejectedExecutionHandler {

		/**
		 * Creates a <tt>BlockPolicy</tt>.
		 */
		public BlockPolicy() {
		}

		/**
		 * Puts the Runnable to the blocking queue, effectively blocking the delegating
		 * thread until space is available.
		 *
		 * @param r
		 *          the runnable task requested to be executed
		 * @param e
		 *          the executor attempting to execute this task
		 */
		public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
			try {
				e.getQueue().put(r);
			} catch (Exception e1) {
				SDFSLogger.getLog()
						.error("Work discarded, thread was interrupted while waiting for space to schedule: {}", e1);
			}
		}
	}

	public static class Shard implements Runnable {
		public byte[] hash;
		public byte[] hashloc;
		public int len;
		public int pos;
		public int apos;
		public int offset;
		public int nlen;
		public boolean direct;

		public byte[] ck;
		AsyncChunkReadActionListener l;

		@Override
		public void run() {
			try {
				if (Arrays.equals(hash, bk)) {
					ck = blankBlock;
					l.commandResponse(this);
				} else {
					this.ck = HCServiceProxy.fetchChunk(hash, hashloc, direct);
					l.commandResponse(this);
				}
			} catch (DataArchivedException e) {
				l.commandArchiveException(e);
			} catch (Throwable e) {
				l.commandException(new Exception(e));
			}

		}

	}

	public static class ShardReader {
		List<Shard> shards;
		public boolean direct;

		public void read() throws IOException, DataArchivedException {
			for (Shard s : shards) {
				if (Arrays.equals(s.hash, bk))
					s.ck = blankBlock;
				else {
					s.ck = HCServiceProxy.fetchChunk(s.hash, s.hashloc, direct);
				}

			}

		}

	}

	@Override
	public void run() {
		try {
			this.close();
		} catch (Exception e) {
			df.errOccured = true;
			SDFSLogger.getLog().error("unable to close", e);
		}

	}

	@Override
	public TreeMap<Integer, HashLocPair> getFingers() {
		// TODO Auto-generated method stub
		return ar;
	}

	@Override
	public boolean getReconstructed() {
		return this.reconstructed;
	}

	public boolean isHlAdded() {
		return hlAdded;
	}

	@Override
	public void setReconstructed(boolean re) {
		this.reconstructed = re;

	}

}