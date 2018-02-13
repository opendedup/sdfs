package org.opendedup.collections;

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

import java.io.File;

import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.io.FileUtils;
import org.opendedup.collections.AbstractHashesMap;
import org.opendedup.collections.AbstractMap;
import org.opendedup.collections.AbstractShard;
import org.opendedup.collections.DataArchivedException;
import org.opendedup.collections.HashtableFullException;
import org.opendedup.collections.KeyNotFoundException;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.hashing.LargeBloomFilter;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.CommandLineProgressBar;
import org.opendedup.util.NextPrime;
import org.opendedup.util.RandomGUID;
import org.opendedup.util.StringUtils;

public class ProgressiveFileBasedCSMap implements AbstractMap, AbstractHashesMap {
	// RandomAccessFile kRaf = null;
	private long size = 0;
	// private final ReentrantLock klock = new ReentrantLock();
	private String fileName;
	private String origFileName;
	long compactKsz = 0;
	private SortedReadMapList maps = null;
	private boolean closed = true;
	private AtomicLong kSz = new AtomicLong(0);
	private long maxSz = 0;
	private byte[] FREE = new byte[HashFunctionPool.hashLength];
	private SDFSEvent loadEvent = SDFSEvent.loadHashDBEvent("Loading Hash Database", Main.mountEvent);
	private long endPos = 0;
	private LargeBloomFilter lbf = null;
	private int hashTblSz = 100000;
	private ArrayList<ProgressiveFileByteArrayLongMap> activeWriteMaps = new ArrayList<ProgressiveFileByteArrayLongMap>();
	private static final int AMS = 1;
	private transient RejectedExecutionHandler executionHandler = new BlockPolicy();
	private transient BlockingQueue<Runnable> worksQueue = new ArrayBlockingQueue<Runnable>(2);

	private transient ThreadPoolExecutor executor = null;
	boolean ilg = false;
	int currentAWPos = 0;
	// private BloomFileByteArrayLongMap activeWMap = null;
	ReentrantLock al = new ReentrantLock();
	private ReentrantReadWriteLock gcLock = new ReentrantReadWriteLock();
	private boolean runningGC = false;

	@Override
	public void init(long maxSize, String fileName, double fpp) throws IOException, HashtableFullException {
		maps = new SortedReadMapList();
		Main.refCount = false;
		this.size = (maxSize);
		this.maxSz = maxSize;
		this.fileName = fileName;
		try {
			this.setUp();
		} catch (Exception e) {
			throw new IOException(e);
		}
		this.closed = false;
		// st = new SyncThread(this);
	}

	// AtomicLong ct = new AtomicLong();
	// AtomicLong mt = new AtomicLong();
	// AtomicLong amt = new AtomicLong();
	// AtomicLong zmt = new AtomicLong();

	private AbstractShard getReadMap(byte[] hash) throws IOException {
		Lock l = gcLock.readLock();
		l.lock();
		// ct.incrementAndGet();
		try {

			if (!runningGC && !lbf.mightContain(hash)) {
				// SDFSLogger.getLog().info("not in bloom filter");
				return null;
			}

			/*
			 * Iterator<ProgressiveFileByteArrayLongMap> iter =
			 * activeReadMaps.iterator(); while (iter.hasNext()) {
			 * ProgressiveFileByteArrayLongMap _m = iter.next(); if
			 * (_m.containsKey(hash)) return _m; }
			 */
			// zmt.incrementAndGet();
			/*
			 * synchronized (ct) { if (ct.get() > 10000) {
			 * SDFSLogger.getLog().info( "misses=" + mt.get() + " attempts=" +
			 * ct.get() + " lookups=" + amt.get()); ct.set(0); amt.set(0);
			 * mt.set(0); } }
			 */
			for (AbstractShard _m : this.maps.getAL()) {
				// amt.incrementAndGet();
				try {
					if (_m.containsKey(hash)) {
						if (runningGC)
							this.lbf.put(hash);
						return _m;
					}
				} catch (MapClosedException e) {
					this.getReadMap(hash);
				}
			}
			// mt.incrementAndGet();

			return null;

		} finally {
			l.unlock();
		}

	}

	private ProgressiveFileByteArrayLongMap createWriteMap() throws IOException {
		ProgressiveFileByteArrayLongMap activeWMap = null;
		try {
			String guid = null;
			boolean written = false;
			while (!written) {
				guid = RandomGUID.getGuid();

				File f = new File(fileName + "-" + guid + ".keys");
				if (!f.exists()) {
					activeWMap = new ProgressiveFileByteArrayLongMap(fileName + "-" + guid, this.hashTblSz);
					activeWMap.activate();
					activeWMap.setUp();
					this.maps.add(activeWMap);

					written = true;
				}
			}
			return activeWMap;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	int cp = 0;

	private ProgressiveFileByteArrayLongMap getWriteMap() throws IOException {
		synchronized (activeWriteMaps) {
			if (cp >= this.activeWriteMaps.size())
				cp = 0;
			ProgressiveFileByteArrayLongMap activeWMap;
			activeWMap = this.activeWriteMaps.get(cp);
			if (activeWMap.isFull() || !activeWMap.isActive()) {
				activeWMap.inActive();
				this.activeWriteMaps.remove(cp);
				activeWMap = this.createWriteMap();
				this.activeWriteMaps.add(cp, activeWMap);
			}
			cp++;
			return activeWMap;
		}
	}

	AtomicLong ict = new AtomicLong();

	private long getPos(byte[] hash) throws IOException {
		long pos = -1;
		Lock l = gcLock.readLock();
		l.lock();
		try {
			if (!runningGC && !lbf.mightContain(hash)) {
				return pos;
			}
			for (AbstractShard m : this.maps.getAL()) {
				try {
					pos = m.get(hash);
				} catch (MapClosedException e) {
					SDFSLogger.getLog().warn("", e);
				}
				if (pos != -1) {
					if (runningGC)
						this.lbf.put(hash);
					// m.cache();
					return pos;
				}
			}
			return pos;
		} finally {
			l.unlock();
		}
	}

	@Override
	public boolean isClosed() {
		return this.closed;
	}

	@Override
	public long getSize() {
		return this.kSz.get();
	}

	@Override
	public long getUsedSize() {

		return this.kSz.get() * Main.CHUNK_LENGTH;
	}

	@Override
	public long getMaxSize() {
		return this.size;
	}

	@Override
	public synchronized long claimRecords(SDFSEvent evt,boolean compact) throws IOException {
		throw new IOException("nor implemented");
	}

	AtomicLong csz = new AtomicLong(0);

	@Override
	public synchronized long claimRecords(SDFSEvent evt, LargeBloomFilter bf) throws IOException {
		if (this.isClosed())
			throw new IOException("Hashtable " + this.fileName + " is close");
		executor = new ThreadPoolExecutor(Main.writeThreads + 1, Main.writeThreads + 1, 10, TimeUnit.SECONDS,
				worksQueue, new ProcessPriorityThreadFactory(Thread.MIN_PRIORITY), executionHandler);
		csz = new AtomicLong(0);

		try {
			Lock l = this.gcLock.writeLock();
			l.lock();
			this.runningGC = true;
			try {
				File _fs = new File(fileName);
				lbf = new LargeBloomFilter(_fs.getParentFile(), maxSz, .01, true, true, false);
			} finally {
				l.unlock();
			}
			SDFSLogger.getLog().info("Claiming Records [" + this.getSize() + "] from [" + this.fileName + "]");
			SDFSEvent tEvt = SDFSEvent
					.claimInfoEvent("Claiming Records [" + this.getSize() + "] from [" + this.fileName + "]", evt);
			tEvt.maxCt = this.maps.size();
			Iterator<AbstractShard> iter = maps.iterator();
			ArrayList<ClaimShard> excs = new ArrayList<ClaimShard>();
			while (iter.hasNext()) {
				tEvt.curCt++;
				AbstractShard m = null;
				try {
					m = iter.next();
					ClaimShard cms = new ClaimShard(m, bf, lbf, csz);
					excs.add(cms);
					executor.execute(cms);
				} catch (Exception e) {
					tEvt.endEvent("Unable to claim records for " + m + " because : [" + e.toString() + "]",
							SDFSEvent.ERROR);
					SDFSLogger.getLog().error("Unable to claim records for " + m, e);
					throw new IOException(e);
				}
			}
			executor.shutdown();
			try {
				while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
					SDFSLogger.getLog().debug("Awaiting fdisk completion of threads.");
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
			for (ClaimShard cms : excs) {
				if (cms.ex != null)
					throw new IOException(cms.ex);
			}
			this.kSz.getAndAdd(-1 * csz.get());
			tEvt.endEvent("removed [" + csz.get() + "] records");
			SDFSLogger.getLog().info("removed [" + csz.get() + "] records");
			iter = maps.iterator();
			while (iter.hasNext()) {
				AbstractShard m = null;
				try {
					m = iter.next();
					if (!m.isFull() && !m.isActive()) {

						// SDFSLogger.getLog().info("deleting " +
						// m.toString());
						m.iterInit();
						KVPair p = m.nextKeyValue();
						while (p != null) {
							ProgressiveFileByteArrayLongMap _m = this.getWriteMap();
							try {
								_m.put(p.key, p.value);
								this.lbf.put(p.key);
								p = m.nextKeyValue();
							} catch (HashtableFullException e) {

							}

						}
						int mapsz = maps.size();
						l = this.gcLock.writeLock();
						l.lock();
						try {
							maps.remove(m);
						} finally {
							l.unlock();
						}
						mapsz = mapsz - maps.size();
						SDFSLogger.getLog()
								.info("removing map " + m.toString() + " sz=" + maps.size() + " rm=" + mapsz);
						m.vanish();

						m = null;
					} else if (m.isMaxed()) {
						SDFSLogger.getLog().info("deleting maxed " + m.toString());
						m.iterInit();
						KVPair p = m.nextKeyValue();
						while (p != null) {
							ProgressiveFileByteArrayLongMap _m = this.getWriteMap();
							try {
								_m.put(p.key, p.value);
								p = m.nextKeyValue();
							} catch (HashtableFullException e) {

							}

						}
						int mapsz = maps.size();
						l = this.gcLock.writeLock();
						l.lock();
						try {
							maps.remove(m);
						} finally {
							l.unlock();
						}
						mapsz = mapsz - maps.size();
						SDFSLogger.getLog()
								.info("removing map " + m.toString() + " sz=" + maps.size() + " rm=" + mapsz);
						m.vanish();

						m = null;
					}
				} catch (Exception e) {
					tEvt.endEvent("Unable to compact " + m + " because : [" + e.toString() + "]", SDFSEvent.ERROR);
					SDFSLogger.getLog().error("to compact " + m, e);
					throw new IOException(e);
				}
			}
			l.lock();
			this.runningGC = false;
			l.unlock();
			return csz.get();
		} finally {
			executor = null;
		}
	}

	private static class ClaimShard implements Runnable {

		AbstractShard map = null;
		LargeBloomFilter bf = null;
		LargeBloomFilter nlbf = null;
		AtomicLong claims = null;
		Exception ex = null;

		protected ClaimShard(AbstractShard map, LargeBloomFilter bf, LargeBloomFilter nlbf, AtomicLong claims) {
			this.map = map;
			this.bf = bf;
			this.claims = claims;
			this.nlbf = nlbf;
		}

		@Override
		public void run() {
			map.iterInit();
			long cl;
			try {
				cl = map.claimRecords(bf, nlbf);
				claims.addAndGet(cl);
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to claim shard", e);
				ex = e;
			}

		}

	}

	public void setMaxSize(long maxSz) throws IOException {
		this.maxSz = maxSz;
		long _tbs = maxSz / (32);
		int max = (Integer.MAX_VALUE / ProgressiveFileByteArrayLongMap.EL) - 100;
		if (_tbs > max) {
			this.hashTblSz = max;
		} else if (_tbs > this.hashTblSz) {
			this.hashTblSz = (int) _tbs;
		}
		long otb = this.hashTblSz;
		this.hashTblSz = NextPrime.getNextPrimeI((int) (this.hashTblSz));
		SDFSLogger.getLog().info("table setup max=" + max + " maxsz=" + this.maxSz + " _tbs=" + _tbs
				+ " calculated hashtblesz=" + otb + " hashTblSz=" + this.hashTblSz);
	}

	/**
	 * initializes the Object set of this hash table.
	 * 
	 * @param initialCapacity
	 *            an <code>int</code> value
	 * @return an <code>int</code> value
	 * @throws HashtableFullException
	 * @throws FileNotFoundException
	 */
	public long setUp() throws Exception {
		File _fs = new File(fileName);
		if (!_fs.getParentFile().exists()) {
			_fs.getParentFile().mkdirs();
		}
		SDFSLogger.getLog().info("Folder = " + _fs.getPath());
		SDFSLogger.getLog().info("Loading freebits bitset");
		long rsz = 0;
		this.setMaxSize(maxSz);
		File[] files = _fs.getParentFile().listFiles(new DBFileFilter());
		if (files.length > 0) {
			CommandLineProgressBar bar = new CommandLineProgressBar("Loading Existing Hash Tables", files.length,
					System.out);
			this.loadEvent.maxCt = files.length + 128;

			for (int i = 0; i < files.length; i++) {
				this.loadEvent.curCt = this.loadEvent.curCt + 1;
				int sz = NextPrime.getNextPrimeI((int) (this.hashTblSz));
				// SDFSLogger.getLog().debug("will create byte array of size "
				// + sz + " propsize was " + propsize);
				ProgressiveFileByteArrayLongMap m = null;
				String pth = files[i].getPath();
				String pfx = pth.substring(0, pth.length() - 5);
				m = new ProgressiveFileByteArrayLongMap(pfx, sz);
				long mep = m.setUp();
				if (mep > endPos)
					endPos = mep;
				maps.add(m);
				rsz = rsz + m.size();
				bar.update(i);
				if (!m.isFull() && this.activeWriteMaps.size() < AMS) {
					m.activate();
					this.activeWriteMaps.add(m);
				} else {
					m.inActive();
					m.full = true;
				}
			}
			bar.finish();
		}

		this.loadEvent.shortMsg = "Loading BloomFilters";

		if (maps.size() != 0 && !LargeBloomFilter.exists(_fs.getParentFile())) {
			lbf = new LargeBloomFilter(_fs.getParentFile(), maxSz, .01, true, true, false);
			SDFSLogger.getLog().warn("Recreating BloomFilters...");
			this.loadEvent.shortMsg = "Recreating BloomFilters";

			executor = new ThreadPoolExecutor(Main.writeThreads, Main.writeThreads, 10, TimeUnit.SECONDS, worksQueue,
					new ProcessPriorityThreadFactory(Thread.MIN_PRIORITY), executionHandler);
			CommandLineProgressBar bar = new CommandLineProgressBar("ReCreating BloomFilters", maps.size(), System.out);
			Iterator<AbstractShard> iter = maps.iterator();
			int i = 0;
			ArrayList<LBFReconstructThread> al = new ArrayList<LBFReconstructThread>();
			while (iter.hasNext()) {
				AbstractShard m = iter.next();
				LBFReconstructThread th = new LBFReconstructThread(lbf, m);
				executor.execute(th);
				al.add(th);
				i++;
				bar.update(i);
			}
			executor.shutdown();
			bar.finish();
			try {
				System.out.print("Waiting for all BloomFilters creation threads to finish");
				while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
					SDFSLogger.getLog().debug("Awaiting fdisk completion of threads.");
					System.out.print(".");

				}
				for (LBFReconstructThread th : al) {
					if (th.ex != null)
						throw th.ex;
				}
				System.out.println(" done");
			} catch (Exception e1) {
				throw new IOException(e1);
			}
		} else {
			lbf = new LargeBloomFilter(_fs.getParentFile(), maxSz, .01, true, true, false);
		}
		while (this.activeWriteMaps.size() < AMS) {
			boolean written = false;
			while (!written) {
				String guid = RandomGUID.getGuid();

				File f = new File(fileName + "-" + guid + ".keys");
				if (!f.exists()) {
					ProgressiveFileByteArrayLongMap activeWMap = new ProgressiveFileByteArrayLongMap(
							fileName + "-" + guid, this.hashTblSz);
					activeWMap.activate();
					activeWMap.setUp();

					this.maps.add(activeWMap);
					written = true;

					this.activeWriteMaps.add(activeWMap);
				}
			}
		}
		this.loadEvent.endEvent("Loaded entries " + rsz);
		System.out.println("Loaded entries " + rsz);
		SDFSLogger.getLog().info("Loaded entries " + rsz);
		SDFSLogger.getLog().info("Loading BloomFilters " + rsz);
		this.kSz.set(rsz);
		this.closed = false;
		return size;
	}

	private static class DBFileFilter implements FileFilter {
		@Override
		public boolean accept(File file) {
			if (file.getPath().endsWith(".keys"))
				return true;
			return false;
		}

	}

	@Override
	public long endStartingPosition() {
		return this.endPos;
	}

	/**
	 * Searches the set for <tt>obj</tt>
	 * 
	 * @param obj
	 *            an <code>Object</code> value
	 * @return a <code>boolean</code> value
	 * @throws IOException
	 */
	@Override
	public boolean containsKey(byte[] key) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		AbstractShard m = this.getReadMap(key);
		if (m == null)
			return false;
		return true;
	}

	@Override
	public InsertRecord put(ChunkData cm) throws IOException, HashtableFullException {
		if (this.isClosed())
			throw new HashtableFullException("Hashtable " + this.fileName + " is close");
		if (this.kSz.get() >= this.maxSz)
			throw new HashtableFullException(
					"entries is greater than or equal to the maximum number of entries. You need to expand"
							+ "the volume or DSE allocation size");
		if (cm.getHash().length != this.FREE.length)
			throw new IOException("key length mismatch");
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		// this.flushFullBuffer();
		return this.put(cm, true);
	}

	// AtomicLong misses = new AtomicLong(0);
	// AtomicLong trs = new AtomicLong(0);
	// AtomicLong msTr = new AtomicLong(0);

	@Override
	public InsertRecord put(ChunkData cm, boolean persist) throws IOException, HashtableFullException {
		// persist = false;
		if (this.isClosed())
			throw new HashtableFullException("Hashtable " + this.fileName + " is close");
		if (kSz.get() >= this.maxSz)
			throw new HashtableFullException("maximum sized reached");
		InsertRecord rec = null;
		// if (persist)
		// this.flushFullBuffer();
		Lock l = gcLock.readLock();
		l.lock();
		ProgressiveFileByteArrayLongMap bm = null;
		try {
			// long tm = System.currentTimeMillis();
			AbstractShard rm = this.getReadMap(cm.getHash());
			if (rm == null) {
				// this.misses.incrementAndGet();
				// tm = System.currentTimeMillis() - tm;
				while (rec == null) {
					try {
						if (persist && !cm.recoverd) {
							try {
								cm.persistData(true);
							} catch (HashExistsException e) {
								return new InsertRecord(false, e.getPos());
							}
						}
						bm = this.getWriteMap();
						rec = bm.put(cm.getHash(), cm.getcPos());
						this.lbf.put(cm.getHash());
					} catch (HashtableFullException e) {
						rec = null;
					} catch (Exception e) {
						throw e;
					}
				}
			} else {
				try {
					rec = new InsertRecord(false, rm.get(cm.getHash()));
				} catch (MapClosedException e) {
					SDFSLogger.getLog().error("unable to remove", e);
				}
			}
			// this.msTr.addAndGet(tm);

		} finally {
			try {
				if (bm != null) {
					bm.activate();
				}
			} catch (Exception e) {

			} finally {
				l.unlock();
			}
		}
		/*
		 * this.trs.incrementAndGet(); if(this.trs.get() == 10000) { long tpm =
		 * 0; if(this.misses.get() > 0) tpm = this.msTr.get()/this.misses.get();
		 * SDFSLogger.getLog().info("trs=" + this.trs.get() + " misses=" +
		 * this.misses.get() + " mtm=" + this.msTr.get() + " tpm=" + tpm);
		 * this.trs.set(0); this.misses.set(0); this.msTr.set(0); }
		 */
		if (rec.getInserted())
			this.kSz.incrementAndGet();
		return rec;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.opendedup.collections.AbstractHashesMap#update(org.opendedup.sdfs
	 * .filestore.ChunkData)
	 */
	@Override
	public boolean update(ChunkData cm) throws IOException {
		boolean added = false;
		AbstractShard bm = this.getReadMap(cm.getHash());
		if (bm != null) {
			added = bm.update(cm.getHash(), cm.getcPos());
		}
		return added;
	}

	public boolean isClaimed(ChunkData cm) throws KeyNotFoundException, IOException {
		throw new IOException("not implemented");
	}

	@Override
	public long get(byte[] key) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		return this.getPos(key);
	}

	@Override
	public byte[] getData(byte[] key) throws IOException, DataArchivedException {
		if (this.isClosed())
			throw new IOException("Hashtable " + this.fileName + " is close");
		long ps = this.get(key);
		if (ps != -1) {
			return ChunkData.getChunk(key, ps);
		} else {
			SDFSLogger.getLog().error("found no data for key [" + StringUtils.getHexString(key) + "]");
			return null;
		}

	}

	@Override
	public byte[] getData(byte[] key, long pos) throws IOException, DataArchivedException {
		if (this.isClosed())
			throw new IOException("Hashtable " + this.fileName + " is close");
		boolean direct = false;
		if (pos == -1) {
			pos = this.get(key);
		} else {
			direct = true;
		}
		if (pos != -1) {
			byte[] data = ChunkData.getChunk(key, pos);
			if (direct && (data == null || data.length == 0)) {
				return this.getData(key);
			} else {
				return data;
			}
		} else {
			SDFSLogger.getLog().error("found no data for key [" + StringUtils.getHexString(key) + "]");
			return null;
		}

	}

	@Override
	public boolean remove(ChunkData cm) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		Lock l = gcLock.readLock();
		l.lock();
		try {
			if (!this.runningGC && !lbf.mightContain(cm.getHash()))
				return false;
			try {
				if (cm.getHash().length == 0)
					return true;
				AbstractShard m = this.getReadMap(cm.getHash());
				if (m == null)
					return false;
				if (!m.remove(cm.getHash())) {
					return false;
				} else {
					cm.setmDelete(true);
					if (this.isClosed()) {
						throw new IOException("hashtable [" + this.fileName + "] is close");
					}
					try {
						this.kSz.decrementAndGet();
					} catch (Exception e) {
					}

					return true;
				}
			} catch (Exception e) {
				SDFSLogger.getLog().fatal("error getting record", e);
				return false;
			}
		} finally {
			l.unlock();
		}
	}

	private ReentrantLock syncLock = new ReentrantLock();

	@Override
	public void sync() throws IOException {
		syncLock.lock();
		try {
			if (this.isClosed()) {
				throw new IOException("hashtable [" + this.fileName + "] is close");
			}
			Iterator<AbstractShard> iter = this.maps.iterator();
			while (iter.hasNext()) {
				try {
					iter.next().sync();
				} catch (IOException e) {
					SDFSLogger.getLog().warn("Unable to sync table", e);
				}
			}
			// this.flushBuffer(true);
			// this.kRaf.getFD().sync();
		} finally {
			syncLock.unlock();
		}
	}

	@Override
	public void close() {
		this.syncLock.lock();
		try {
			this.closed = true;

			CommandLineProgressBar bar = new CommandLineProgressBar("Closing Hash Tables", this.maps.size(),
					System.out);
			Iterator<AbstractShard> iter = this.maps.iterator();
			int i = 0;
			while (iter.hasNext()) {
				try {

					iter.next().close();
					bar.update(i++);
				} catch (Exception e) {
					SDFSLogger.getLog().warn("Unable to close table", e);
				}
			}
			bar.finish();
			maps = null;
			try {
				this.lbf.save(new File(fileName).getParentFile());
			} catch (Throwable e) {
				SDFSLogger.getLog().warn("unable to save bloomfilter", e);
			}
		} finally {
			this.syncLock.unlock();
			SDFSLogger.getLog().info("Hashtable [" + this.fileName + "] closed");
		}
	}

	@Override
	public void vanish() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void initCompact() throws IOException {
		this.close();
		this.origFileName = fileName;
		String parent = new File(this.fileName).getParentFile().getPath();
		String fname = new File(this.fileName).getName();
		this.fileName = parent + ".compact" + File.separator + fname;
		File f = new File(this.fileName).getParentFile();
		if (f.exists()) {
			FileUtils.deleteDirectory(f);
		}
		FileUtils.copyDirectory(new File(parent), new File(parent + ".compact"));

		try {
			this.init(maxSz, this.fileName, .001);
		} catch (Exception e) {
			throw new IOException(e);
		}

	}

	@Override
	public void commitCompact(boolean force) throws IOException {
		this.close();
		FileUtils.deleteDirectory(new File(this.origFileName).getParentFile());
		SDFSLogger.getLog().info("Deleted " + new File(this.origFileName).getParent());
		new File(this.fileName).getParentFile().renameTo(new File(this.origFileName).getParentFile());
		SDFSLogger.getLog().info(
				"moved " + new File(this.fileName).getParent() + " to " + new File(this.origFileName).getParent());
		FileUtils.deleteDirectory(new File(this.fileName).getParentFile());
		SDFSLogger.getLog().info("deleted " + new File(this.fileName).getParent());

	}

	@Override
	public void rollbackCompact() throws IOException {
		FileUtils.deleteDirectory(new File(this.fileName));

	}

	protected final static class ProcessPriorityThreadFactory implements ThreadFactory {

		private final int threadPriority;

		public ProcessPriorityThreadFactory(int threadPriority) {
			this.threadPriority = threadPriority;
		}

		@Override
		public Thread newThread(Runnable r) {
			Thread thread = new Thread(r);
			thread.setPriority(threadPriority);
			return thread;
		}

	}

	@Override
	public void cache(long pos) throws IOException {
			try {

				ChunkData.cacheChunk(pos);
			} catch (Exception e) {
				SDFSLogger.getLog().debug("error getting [" + pos + "]", e);
			}

	}

	@Override
	public boolean mightContainKey(byte[] key,long id) {
		if (!this.runningGC)
			return this.lbf.mightContain(key);
		else {
			long ps = -1;
			try {
				ps = this.get(key);
			} catch (IOException e) {
				SDFSLogger.getLog().warn("unable to check", e);
				return true;
			}
			if (ps != -1) {
				return true;
			} else
				return false;
		}
	}

	@Override
	public boolean claimKey(byte[] key, long val,long claims) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clearRefMap() throws IOException {
		// TODO Auto-generated method stub

	}
}