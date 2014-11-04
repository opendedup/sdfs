package org.opendedup.sdfs.io;



import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.collections.HashtableFullException;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.hashing.AbstractHashEngine;
import org.opendedup.hashing.Finger;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.hashing.VariableHashEngine;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.HCServiceProxy;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * 
 * @author annesam WritableCacheBuffer is used to store written data for later
 *         writing and reading by the DedupFile. WritableCacheBuffers are
 *         evicted from the file system based LRU. When a writable cache buffer
 *         is evicted it is then written to the dedup chunk service
 */
public class WritableCacheBuffer implements DedupChunkInterface, Runnable {

	private ByteBuffer buf = null;
	private boolean dirty = false;

	private long endPosition = 0;
	// private int currentLen = 0;
	private int length;
	private long position;
	private boolean newChunk = false;
	private boolean writable = false;
	private int doop = 0;
	private int bytesWritten = 0;
	private SparseDedupFile df;
	private final ReentrantLock lock = new ReentrantLock();
	private boolean closed = true;
	private boolean flushing = false;
	boolean rafInit = false;
	int prevDoop = 0;
	private boolean batchprocessed;
	private boolean batchwritten;
	private boolean reconstructed;
	private boolean hlAdded = false;
	private List<HashLocPair> ar = new ArrayList<HashLocPair>();
	int sz;
	private static int maxTasks = ((Main.maxWriteBuffers * 1024 * 1024) / (Main.CHUNK_LENGTH)) + 1;
	private static BlockingQueue<Runnable> worksQueue = null;
	private static RejectedExecutionHandler executionHandler = new BlockPolicy();
	private static ThreadPoolExecutor executor = null;
	private static BlockingQueue<Runnable> lworksQueue = null;
	private static RejectedExecutionHandler lexecutionHandler = new BlockPolicy();
	private static ThreadPoolExecutor lexecutor = null;
	static {
		if (maxTasks > 120)
			maxTasks = 120;
		SDFSLogger.getLog().info(
				"WriteCacheBuffer Pool List Size will be " + maxTasks);
		worksQueue = new LinkedBlockingQueue<Runnable>(maxTasks);
		executor = new ThreadPoolExecutor(Main.writeThreads, Main.writeThreads,
				10, TimeUnit.SECONDS, worksQueue, executionHandler);
		lworksQueue = new LinkedBlockingQueue<Runnable>(maxTasks);
		lexecutor = new ThreadPoolExecutor(Main.writeThreads,
				Main.writeThreads, 10, TimeUnit.SECONDS, lworksQueue,
				lexecutionHandler);

		executor.allowCoreThreadTimeOut(true);
	}

	public WritableCacheBuffer(long startPos, int length, SparseDedupFile df,
			List<HashLocPair> ar, boolean reconstructed) throws IOException {
		this.length = length;
		this.position = startPos;
		this.newChunk = true;
		this.ar = ar;
		this.df = df;
		this.reconstructed = reconstructed;
		buf = ByteBuffer.wrap(new byte[Main.CHUNK_LENGTH]);
		
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

	public WritableCacheBuffer(DedupChunkInterface dk, DedupFile df)
			throws IOException {
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

	public byte[] getReadChunk(int startPos, int len) throws IOException,
			BufferClosedException {
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug(
					"reading " + df.getMetaFile().getPath() + " df="
							+ df.getGUID() + " fpos=" + this.position
							+ " start=" + startPos + " len=" + len);

		if (this.closed)
			throw new BufferClosedException("Buffer Closed");
		if (this.flushing)
			throw new BufferClosedException("Buffer Flushing");
		this.lock.lock();
		try {
			try {
				this.initBuffer();
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
			byte[] dd = new byte[len];
			buf.position(startPos);
			buf.get(dd);
			if (SDFSLogger.isDebug()) {
				if (df.getMetaFile().getPath().endsWith(".vmx")
						|| df.getMetaFile().getPath().endsWith(".vmx~")) {
					SDFSLogger.getLog().debug(
							"###### In wb read Text of VMX="
									+ df.getMetaFile().getPath() + "="
									+ new String(dd, "UTF-8"));
				}
			}
			return dd;
		} finally {
			this.lock.unlock();
		}

	}

	private void initBuffer() throws IOException, InterruptedException {
		if (this.buf == null) {
			if (HashFunctionPool.max_hash_cluster > 1) {
				ByteBuffer hcb = ByteBuffer.wrap(new byte[Main.CHUNK_LENGTH]);
				final ArrayList<Shard> cks = new ArrayList<Shard>();
				int i = 0;
				for (HashLocPair p : ar) {

					if (p.hashloc[1] != 0) {
						Shard sh = new Shard();
						sh.hash = p.hash;
						sh.hashloc = p.hashloc;
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
						this.incrementAndGetDNEX();
						SDFSLogger.getLog()
								.error("Error while getting hash", e);
						if (this.incrementandGetDN() >= sz) {
							synchronized (this) {
								this.notifyAll();
							}
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

				};
				for (Shard sh : cks) {
					sh.l = l;
					executor.execute(sh);
				}
				int wl = 0;
				int tm = 1000;
				int al = 0;
				while (l.getDN() < sz) {
					if (al == 30) {
						int nt = wl / 1000;
						SDFSLogger
								.getLog()
								.warn("Slow io, waited ["
										+ nt
										+ "] seconds for all reads to complete.");
						al = 0;
					}
					if (Main.readTimeoutSeconds > 0
							&& wl > (Main.writeTimeoutSeconds * tm)) {
						int nt = (tm * wl) / 1000;
						throw new IOException("read Timed Out after [" + nt
								+ "] seconds. Expected [" + sz
								+ "] block read but only [" + l.getDN()
								+ "] were completed");
					}
					synchronized (l) {
						l.wait(1000);
					}
					wl += 1000;
					al++;
				}
				if (l.getDN() < sz)
					SDFSLogger.getLog().warn(
							"thread timed out before read was complete ");
				if(l.getDNEX() > 0)
					throw new IOException("error while getting blocks " + l.getDNEX() + " errors found");
				hcb.position(0);
				for (Shard sh : cks) {

					try {
						hcb.position(sh.pos);
						hcb.put(sh.ck, sh.offset, sh.nlen);
					} catch (Exception e) {
						SDFSLogger.getLog().error(
								"pos = " + this.position + " ck nlen="
										+ sh.nlen + " ck offset=" + sh.offset
										+ " ck len=" + sh.ck.length
										+ " hcb pos=" + hcb.position()
										+ " ck slen=" + sh.len + " len="
										+ (hcb.capacity()));
						throw new IOException(e);
					}
				}
				this.buf = ByteBuffer.wrap(hcb.array());
			} else {
				this.buf = ByteBuffer.wrap(HCServiceProxy.fetchChunk(
						this.ar.get(0).hash, this.ar.get(0).hashloc));
				/*
				 * if(SDFSLogger.isDebug()) { try { AbstractHashEngine eng =
				 * HashFunctionPool.getHashEngine(); byte [] _hash =
				 * eng.getHash(this.buf.array()); if(!Arrays.areEqual(_hash,
				 * this.hash)) {
				 * SDFSLogger.getLog().debug("data is not consistent _hash=" +
				 * StringUtils.getHexString(_hash) + " and hash=" +
				 * StringUtils.getHexString(this.hash)); }
				 * 
				 * } catch (Exception e) { // TODO Auto-generated catch block
				 * SDFSLogger.getLog().error("error hashing in debug mode", e);
				 * } }
				 */
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#capacity()
	 */
	@Override
	public int capacity() {
		this.lock.lock();
		try {
			if (buf != null) {
				return this.buf.capacity();
			} else {
				return Main.CHUNK_LENGTH;
			}
		} finally {
			this.lock.unlock();
		}
	}

	public void setAR(List<HashLocPair> al) {
		this.ar = al;
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

	public void putHash(HashLocPair p) throws IOException {
		this.lock.lock();
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
					//SDFSLogger.getLog().info("0 removing h = " + h);
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
						if (_h.isInvalid()) {
							SDFSLogger.getLog().error("zoffset = " + offset);
							SDFSLogger.getLog().error("p = " + p.toString());
							SDFSLogger.getLog().error("h = " + h.toString());
						}
						//SDFSLogger.getLog().info("1 add _h = " + _h);
						am.add(_h);
					}
					if (h.pos < p.pos) {
						h.nlen = (p.pos - h.pos);
						//SDFSLogger.getLog().info("2 add h = " + h);
					} else {
						//SDFSLogger.getLog().info("3 rm h = " + h);
						if (rm == null)
							rm = new ArrayList<HashLocPair>();
						rm.add(h);
					}
				} else if (h.pos > p.pos && h.pos < ep) {
					int no = ep - h.pos;
					HashLocPair _h = h.clone();
					h.pos = ep;
					h.offset += no;
					h.nlen -= no;
					if (h.isInvalid()) {
						SDFSLogger.getLog().error("offset = " + no);
						SDFSLogger.getLog().error("p = " + p.toString());
						SDFSLogger.getLog().error("h = " + _h.toString());
					}
					//SDFSLogger.getLog().info("4 update h = " + h);
				} 
				if (h.isInvalid()) {
					//SDFSLogger.getLog().error("h = " + h.toString());
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
			ar.add(p);
			Collections.sort(ar);
			this.hlAdded = true;
			this.reconstructed = true;
		} finally {
			this.lock.unlock();
		}
	}

	private void writeBlock(byte[] b, int pos) throws IOException {
		try {
			this.initBuffer();
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
		buf.position(pos);
		buf.put(b);
		this.setDirty(true);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#write(byte[], int)
	 */

	@Override
	public void write(byte[] b, int pos) throws BufferClosedException,
			IOException {
		if (SDFSLogger.isDebug()) {
			SDFSLogger.getLog().debug(
					"writing " + df.getMetaFile().getPath() + "df="
							+ df.getGUID() + "fpos=" + this.position + " pos="
							+ pos + " len=" + b.length);
			if (df.getMetaFile().getPath().endsWith(".vmx")
					|| df.getMetaFile().getPath().endsWith(".vmx~")) {
				SDFSLogger.getLog().debug(
						"###### In wb Text of VMX="
								+ df.getMetaFile().getPath() + "="
								+ new String(b, "UTF-8"));
			}
		}
		this.lock.lock();
		try {
			if (this.closed)
				throw new BufferClosedException("Buffer Closed while writing");
			if (this.flushing)
				throw new BufferClosedException("Buffer Flushing");

			/*
			 * if(pos != 0) SDFSLogger.getLog().info("start at " + pos);
			 * if(b.length != this.capacity())
			 * SDFSLogger.getLog().info("!capacity " + b.length);
			 */
			if (pos == 0 && b.length == Main.CHUNK_LENGTH) {
				this.buf = ByteBuffer.wrap(b);
				this.setDirty(true);
			} else {
				if (this.buf == null && ar.size() > 0 && this.reconstructed
						&& HashFunctionPool.max_hash_cluster > 1) {
					long _spos = this.getChuckPosition(pos);
					int bpos =(int) (pos - _spos);
					//SDFSLogger.getLog().info("poop " + b.length +  " pos=" + pos + "_spos=" + _spos + " bpos=" +bpos );
					if (b.length < VariableHashEngine.minLen) {
						HashLocPair p = new HashLocPair();
						AbstractHashEngine eng = SparseDedupFile.hashPool
								.borrowObject();
						try {
							p.hash = eng.getHash(b);
							p.hashloc = HCServiceProxy.writeChunk(p.hash, b,
									true);
							p.len = b.length;
							p.nlen = b.length;
							p.offset = 0;
							p.pos = bpos;
							int dups = 0;
							if (p.hashloc[0] == 1)
								dups = b.length;
							df.mf.getIOMonitor().addVirtualBytesWritten(
									b.length, true);
							df.mf.getIOMonitor()
									.addActualBytesWritten(
											Main.CHUNK_LENGTH
													- (dups + this
															.getPrevDoop()),
											true);
							df.mf.getIOMonitor().addDulicateData(
									(dups + this.getPrevDoop()), true);
							this.prevDoop += dups;
							this.putHash(p);
							if (this.ar.size() > LongByteArrayMap.MAX_ELEMENTS_PER_AR)
								this.writeBlock(b, pos);
							/*
							HashLocPair _h =null;
							
							for(HashLocPair h : ar) {
								if(_h!=null && h.pos != (_h.pos + _h.nlen)) {
									SDFSLogger.getLog().info("data mismatch");
									SDFSLogger.getLog().info(_h);
									SDFSLogger.getLog().info(h);
								}
								_h=h;
							}
							*/

						} catch (HashtableFullException e) {
							SDFSLogger.getLog().error(
									"unable to write with accelerator", e);
							throw new IOException(e);
						} finally {
							SparseDedupFile.hashPool.returnObject(eng);
						}
					} else {
						this.wm(b, bpos);
						/*
						HashLocPair _h =null;
						
						for(HashLocPair h : ar) {
							if(_h!=null && h.pos != (_h.pos + _h.nlen)) {
								SDFSLogger.getLog().info("data mismatch");
								SDFSLogger.getLog().info(_h);
								SDFSLogger.getLog().info(h);
							}
							_h=h;
						}
						*/
					}
				} else {
					this.writeBlock(b, pos);
				}
			}
			
			this.bytesWritten = this.bytesWritten + b.length;
		} finally {
			this.lock.unlock();
		}
	}

	private void wm(byte[] b, int pos) throws IOException {
		VariableHashEngine hc = (VariableHashEngine) SparseDedupFile.hashPool
				.borrowObject();
		int zpos = pos;
		try {
			List<Finger> fs = hc.getChunks(b);
			AsyncChunkWriteActionListener l = new AsyncChunkWriteActionListener() {

				@Override
				public void commandException(Finger result, Throwable e) {
					int _dn = this.incrementandGetDN();
					this.incrementAndGetDNEX();
					SDFSLogger.getLog().error("Error while getting hash", e);
					if (_dn >= this.getMaxSz()) {
						synchronized (this) {
							this.notifyAll();
						}
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

			};
			l.setMaxSize(fs.size());
			for (Finger f : fs) {
				f.l = l;
				f.dedup = df.mf.isDedup();
				SparseDedupFile.executor.execute(f);
			}
			int wl = 0;
			int tm = 1000;

			int al = 0;
			while (l.getDN() < fs.size()) {
				if (al == 30) {
					int nt = wl / 1000;
					SDFSLogger.getLog().warn(
							"Slow io, waited [" + nt
									+ "] seconds for all writes to complete.");
					al = 0;
				}
				if (Main.writeTimeoutSeconds > 0
						&& wl > (Main.writeTimeoutSeconds * tm)) {
					int nt = (tm * wl) / 1000;
					df.toOccured = true;
					throw new IOException("Write Timed Out after [" + nt
							+ "] seconds. Expected [" + fs.size()
							+ "] block writes but only [" + l.getDN()
							+ "] were completed");
				}
				synchronized (l) {
					l.wait(tm);
				}
				al++;
				wl += tm;
			}
			if (l.getDN() < fs.size()) {
				df.toOccured = true;
				throw new IOException("Write Timed Out expected [" + fs.size()
						+ "] but got [" + l.getDN() + "]");
			}
			if (l.getDNEX() > 0)
				throw new IOException("Write Failed");
			for (Finger f : fs) {
				HashLocPair p = new HashLocPair();
				try {
					p.hash = f.hash;
					p.hashloc = f.hl;
					p.len = f.len;
					p.offset = 0;
					p.nlen = f.len;
					p.pos = pos;
					pos += f.len;
					int dups = 0;
					if (p.hashloc[0] == 1)
						dups = b.length;
					df.mf.getIOMonitor().addVirtualBytesWritten(b.length, true);
					df.mf.getIOMonitor().addActualBytesWritten(
							Main.CHUNK_LENGTH - (dups + this.getPrevDoop()),
							true);
					df.mf.getIOMonitor().addDulicateData(
							(dups + this.getPrevDoop()), true);
					this.prevDoop += dups;
					this.putHash(p);
				} catch (Throwable e) {
					SDFSLogger.getLog()
							.warn("unable to write object finger", e);
					throw e;
					// SDFSLogger.getLog().info("this chunk size is "
					// + f.chunk.length);
				}
			}
			if (this.ar.size() > LongByteArrayMap.MAX_ELEMENTS_PER_AR)
				this.writeBlock(b, zpos);
		} catch (Throwable e) {
			df.errOccured = true;
			throw new IOException(e);
		} finally {
			SparseDedupFile.hashPool.returnObject(hc);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#truncate(int)
	 */
	@Override
	public void truncate(int len) throws BufferClosedException {
		this.lock.lock();
		try {

			if (this.closed)
				throw new BufferClosedException("Buffer Closed");
			
				this.destroy();
			this.setDirty(true);
		} finally {
			this.lock.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#isDirty()
	 */
	@Override
	public boolean isDirty() {
		this.lock.lock();
		try {
			return dirty;
		} finally {
			this.lock.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#setDirty(boolean)
	 */
	@Override
	public void setDirty(boolean dirty) {
		this.lock.lock();
		this.dirty = dirty;
		this.lock.unlock();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#toString()
	 */
	@Override
	public String toString() {
		return this.hashCode() + ":" + this.getFilePosition() + ":"
				+ this.getLength() + ":" + this.getEndPosition();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#open()
	 */
	@Override
	public void open() {
		try {
			this.lock.lock();
			if (this.flushing) {
				this.df.removeBufferFromFlush(this);
			}
			this.closed = false;
			this.flushing = false;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Error while opening", e);
			throw new IllegalArgumentException("error");
		} finally {
			this.lock.unlock();
		}
	}

	public void flush() throws BufferClosedException {
		this.lock.lock();
		try {

			if (this.flushing) {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug(
							"cannot flush buffer at pos "
									+ this.getFilePosition()
									+ " already flushing");
				throw new BufferClosedException("Buffer Closed");

			}
			if (this.closed) {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug(
							"cannot flush buffer at pos "
									+ this.getFilePosition() + " closed");
				throw new BufferClosedException("Buffer Closed");
			}
			this.flushing = true;
			if (this.isDirty()) {
				this.df.putBufferIntoFlush(this);
				if (Main.chunkStoreLocal)
					lexecutor.execute(this);
				else
					SparseDedupFile.pool.execute(this);
			}
		} finally {
			this.lock.unlock();
		}
	}

	public boolean isClosed() {
		this.lock.lock();
		try {
			return this.closed;
		} finally {
			this.lock.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#close()
	 */
	@Override
	public void close() throws IOException {
		this.lock.lock();
		try {

			if (!this.flushing) {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog()
							.debug("#### " + this.getFilePosition()
									+ " not flushing ");
			}

			else if (this.closed) {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug(
							this.getFilePosition() + " already closed");
			} else if (this.dirty) {
				this.df.writeCache(this);
				df.removeBufferFromFlush(this);
				this.closed = true;
				this.flushing = false;
				this.dirty = false;

			} else {
				this.closed = true;
				this.flushing = false;
			}
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			this.lock.unlock();

		}
	}

	public void startClose() {
		this.lock.lock();
		this.batchprocessed = true;
	}

	public boolean isBatchProcessed() {
		return this.batchprocessed;
	}

	public void endClose() throws IOException {
		try {
			if (!this.flushing) {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug(
							"####" + this.getFilePosition() + " not flushing");
			} else if (this.closed) {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug(
							this.getFilePosition() + " already closed");
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
			this.lock.unlock();

		}
	}

	public byte[] getFlushedBuffer() throws BufferClosedException {
		this.lock.lock();
		try {
			if (this.closed) {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug(
							this.getFilePosition() + " already closed");
				throw new BufferClosedException("Buffer Closed");
			}
			if (!this.flushing) {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug(
							this.getFilePosition() + " not flushed");
				throw new BufferClosedException("Buffer not flushed");
			}
			if (this.buf == null)
				SDFSLogger.getLog().info(
						this.getFilePosition() + " buffer is null");
			byte[] b = new byte[this.buf.capacity()];
			System.arraycopy(this.buf.array(), 0, b, 0, b.length);
			return b;
		} finally {
			this.lock.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#persist()
	 */
	@Override
	public void persist() {
		try {
			this.lock.lock();
			this.df.writeCache(this);
			this.closed = true;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Error while closing", e);
			throw new IllegalArgumentException("error while closing "
					+ e.toString());
		} finally {
			this.lock.unlock();
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
		this.lock.lock();
		try {
			HashFunction hf = Hashing.murmur3_128(6442);
			return hf.hashBytes(buf.array()).asInt();
		} finally {
			this.lock.unlock();
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
		 * Puts the Runnable to the blocking queue, effectively blocking the
		 * delegating thread until space is available.
		 * 
		 * @param r
		 *            the runnable task requested to be executed
		 * @param e
		 *            the executor attempting to execute this task
		 */
		public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
			try {
				e.getQueue().put(r);
			} catch (Exception e1) {
				SDFSLogger
						.getLog()
						.error("Work discarded, thread was interrupted while waiting for space to schedule: {}",
								e1);
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
		byte[] ck;
		AsyncChunkReadActionListener l;

		@Override
		public void run() {
			try {
				this.ck = Arrays.copyOf(
						HCServiceProxy.fetchChunk(hash, hashloc), len);
				l.commandResponse(this);
			} catch (Exception e) {
				l.commandException(e);
			}
		}
	}

	@Override
	public void run() {
		try {
			this.close();
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to close", e);
		}

	}

	@Override
	public List<HashLocPair> getFingers() {
		// TODO Auto-generated method stub
		Collections.sort(ar);
		return ar;
	}

	@Override
	public boolean getReconstructed() {
		return this.reconstructed;
	}

	public boolean isHlAdded() {
		return hlAdded;
	}

	public void setHlAdded(boolean hlAdded) {
		this.hlAdded = hlAdded;
	}
	
	private long getChuckPosition(long location) {
		long place = location / Main.CHUNK_LENGTH;
		place = place * Main.CHUNK_LENGTH;
		return place;
	}

	@Override
	public void setReconstructed(boolean re) {
		this.reconstructed = re;
		
	}

}
