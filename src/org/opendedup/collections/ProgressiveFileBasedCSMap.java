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

import java.io.File;

import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.opendedup.hashing.LargeFileBloomFilter;
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
	protected SortedReadMapList maps = null;
	protected boolean closed = true;
	private AtomicLong kSz = new AtomicLong(0);
	private long maxSz = 0;
	private byte[] FREE = new byte[HashFunctionPool.hashLength];
	private SDFSEvent loadEvent = SDFSEvent.loadHashDBEvent("Loading Hash Database", Main.mountEvent);
	private long endPos = 0;
	protected LargeBloomFilter lbf = null;
	private int hashTblSz = 100000;
	protected int lhashTblSz = 100000;
	private boolean hugeTables = false;
	private ArrayList<ProgressiveFileByteArrayLongMap> activeWriteMaps = new ArrayList<ProgressiveFileByteArrayLongMap>();
	private static final int AMS = Main.parallelDBCount;
	private transient RejectedExecutionHandler executionHandler = new BlockPolicy();
	private transient BlockingQueue<Runnable> worksQueue = new SynchronousQueue<Runnable>();
	private transient ProgressiveFileByteArrayLongMap lactiveWMap = null;
	Thread cdth = null;
	private transient ThreadPoolExecutor executor = null;
	boolean ilg = false;
	int currentAWPos = 0;
	// private BloomFileByteArrayLongMap activeWMap = null;
	ReentrantLock al = new ReentrantLock();
	protected ReentrantReadWriteLock gcLock = new ReentrantReadWriteLock();
	private boolean runningGC = false;
	//private long lastInsert = System.currentTimeMillis();
	private LinkedHashMap<ByteArrayWrapper, ProgressiveFileByteArrayLongMap> keyLookup = new LinkedHashMap<ByteArrayWrapper, ProgressiveFileByteArrayLongMap>(
			2000000, 0.75f, true) {
		/**
		* 
		*/
		private static final long serialVersionUID = 1L;

		protected boolean removeEldestEntry(Map.Entry<ByteArrayWrapper, ProgressiveFileByteArrayLongMap> eldest) {
			return size() > 2000000;
		}
	};

	@Override
	public void init(long maxSize, String fileName) throws IOException, HashtableFullException {
		maps = new SortedReadMapList();

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

	AtomicLong mt = new AtomicLong(0);
	AtomicLong mttm = new AtomicLong(0);
	AtomicLong ct = new AtomicLong(0);
	AtomicLong fd = new AtomicLong(0);
	// AtomicLong ch = new AtomicLong(0);
	/*
	 * private ProgressiveFileByteArrayLongMap getReadMap(byte[] hash) throws
	 * IOException { try { return this.keyLookup.get(new
	 * ByteArrayWrapper(hash)); }catch(Exception e) { try { throw e.getCause();
	 * }catch(NotFoundException e1) { return null; } catch(Throwable e1) { throw
	 * new IOException(e1); } } }
	 */

	private ProgressiveFileByteArrayLongMap getReadMap(byte[] hash) throws IOException {
		Lock l = gcLock.readLock();
		l.lock();
		long count = ct.incrementAndGet();
		try {

			if (!runningGC && !lbf.mightContain(hash)) {
				// SDFSLogger.getLog().info("not in bloom filter");
				return null;
			}
			ProgressiveFileByteArrayLongMap _km;
			_km = this.keyLookup.get(new ByteArrayWrapper(hash));
			if (_km != null) {
				// long chl = ch.incrementAndGet();
				// SDFSLogger.getLog().info("found ch="+chl + " sz="
				// +this.keyLookup.size());
				return _km;
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
			long tm = System.currentTimeMillis();
			int sns = 0;
			for (ProgressiveFileByteArrayLongMap _m : this.maps.getAL()) {
				sns++;
				if (_m.containsKey(hash)) {
					if (runningGC)
						this.lbf.put(hash);
					long z = fd.incrementAndGet();
					if (!_m.isActive()) {
						synchronized (this.keyLookup) {
							this.keyLookup.put(new ByteArrayWrapper(hash), _m);
						}
						SDFSLogger.getLog()
								.info("ct=" + count + " found fd=" + z + " scans=" + sns + " time="
										+ (System.currentTimeMillis() - tm) + " actve=" + _m.isActive() + " sz="
										+ this.keyLookup.size());
					}
					return _m;
				}
			}
			long k = mttm.addAndGet(System.currentTimeMillis() - tm);
			long z = mt.incrementAndGet();
			if (z > 100) {
				long atime = k / z;
				SDFSLogger.getLog().info("misses = " + z + " average seek time=" + atime + " total seektime=" + k);
				mt.set(0);
				mttm.set(0);
			}
			// long atime = k / z;
			// SDFSLogger.getLog().info("misses = " + z + " average seek time="
			// + atime + " total seektime=" + k);
			return null;

		} finally {
			l.unlock();
		}
	}

	private ProgressiveFileByteArrayLongMap createWriteMap(int sz) throws IOException {
		ProgressiveFileByteArrayLongMap activeWMap = null;
		try {
			String guid = null;
			boolean written = false;
			while (!written) {
				guid = RandomGUID.getGuid();

				File f = new File(fileName + "-" + guid + ".keys");
				if (!f.exists()) {
					synchronized (this) {
						activeWMap = new ProgressiveFileByteArrayLongMap(fileName + "-" + guid, sz);
						activeWMap.setUp();
						activeWMap.activate();
						maps.sort();
					}
					if (sz == this.lhashTblSz)
						activeWMap.initialize();
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
			if (activeWMap.isFull()) {
				activeWMap.inActive();
				this.activeWriteMaps.remove(cp);
				activeWMap = this.createWriteMap(this.hashTblSz);
				this.activeWriteMaps.add(cp, activeWMap);
			}
			cp++;
			return activeWMap;
		}
	}

	protected ProgressiveFileByteArrayLongMap getLargeWriteMap() throws IOException {
		synchronized (lactiveWMap) {
			if (lactiveWMap.isFull()) {
				lactiveWMap.inActive();
				lactiveWMap = this.createWriteMap(this.lhashTblSz);
			}
			return lactiveWMap;
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
			for (ProgressiveFileByteArrayLongMap m : this.maps.getAL()) {
				pos = m.get(hash);
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
	public synchronized void claimRecords(SDFSEvent evt) throws IOException {
		throw new IOException("nor implemented");
	}

	AtomicLong csz = new AtomicLong(0);

	@Override
	public synchronized long claimRecords(SDFSEvent evt, LargeFileBloomFilter bf) throws IOException {
		if (this.isClosed())
			throw new IOException("Hashtable " + this.fileName + " is close");
		executor = new ThreadPoolExecutor(Main.writeThreads, Main.writeThreads, 10, TimeUnit.SECONDS, worksQueue,
				new ProcessPriorityThreadFactory(Thread.MIN_PRIORITY), executionHandler);
		csz = new AtomicLong(0);
		try {
			Lock l = this.gcLock.writeLock();
			l.lock();
			this.runningGC = true;
			try {
				File _fs = new File(fileName);
				lbf = null;
				lbf = new LargeBloomFilter(_fs.getParentFile(), maxSz, .01, false, true);
			} finally {
				l.unlock();
			}
			SDFSLogger.getLog().info("Claiming Records [" + this.getSize() + "] from [" + this.fileName + "]");
			SDFSEvent tEvt = SDFSEvent
					.claimInfoEvent("Claiming Records [" + this.getSize() + "] from [" + this.fileName + "]", evt);
			tEvt.maxCt = this.maps.size();
			Iterator<ProgressiveFileByteArrayLongMap> iter = maps.iterator();
			ArrayList<ClaimShard> excs = new ArrayList<ClaimShard>();
			while (iter.hasNext()) {
				tEvt.curCt++;
				ProgressiveFileByteArrayLongMap m = null;
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
			iter = maps.getLMMap().iterator();
			while (iter.hasNext()) {
				ProgressiveFileByteArrayLongMap m = null;
				try {
					m = iter.next();
					boolean cp = !m.isFull() && !m.isActive() && !m.isCompactig() && !this.closed;
					boolean bp = !m.isCompactig() && m != null && m.maxSize() < this.lhashTblSz && !this.closed
							&& !m.isClosed();
					if (cp || bp) {
						// SDFSLogger.getLog().info("deleting " +
						// m.toString());
						m.iterInit();
						KVPair p = m.nextKeyValue();
						while (p != null) {
							ProgressiveFileByteArrayLongMap _m = this.getLargeWriteMap();
							try {
								_m.put(p.key, p.value);
								ByteArrayWrapper bw = new ByteArrayWrapper(p.key);

								if (this.keyLookup.containsKey(bw)) {
									synchronized (keyLookup) {
										this.keyLookup.put(bw, _m);
									}
								}
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
					} else if (m.isMaxed() && !m.isCompactig() && !this.closed && !m.isClosed()) {
						SDFSLogger.getLog().info("deleting maxed " + m.toString());
						m.iterInit();
						KVPair p = m.nextKeyValue();
						while (p != null) {
							ProgressiveFileByteArrayLongMap _m = this.getWriteMap();
							try {
								_m.put(p.key, p.value);
								ByteArrayWrapper bw = new ByteArrayWrapper(p.key);

								if (this.keyLookup.containsKey(bw)) {
									synchronized (keyLookup) {
										this.keyLookup.put(bw, _m);
									}
								}
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
								.info("removing maxed map " + m.toString() + " sz=" + maps.size() + " rm=" + mapsz);
						m.vanish();
						this.keyLookup.clear();
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

		ProgressiveFileByteArrayLongMap map = null;
		LargeFileBloomFilter bf = null;
		LargeBloomFilter nlbf = null;
		AtomicLong claims = null;
		Exception ex = null;

		protected ClaimShard(ProgressiveFileByteArrayLongMap map, LargeFileBloomFilter bf, LargeBloomFilter nlbf,
				AtomicLong claims) {
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
		int max = Integer.MAX_VALUE / (ProgressiveFileByteArrayLongMap.EL * 4);
		int lmax = (AMS * max * 2);
		if (_tbs > max) {
			this.hashTblSz = max;
			this.lhashTblSz = lmax;
			this.hugeTables = true;
		} else if (_tbs > this.hashTblSz) {
			this.hashTblSz = (int) _tbs;
		}
		if (_tbs < lmax)
			this.lhashTblSz = (int) _tbs;

		long otb = this.hashTblSz;
		this.hashTblSz = NextPrime.getNextPrimeI((int) (this.hashTblSz));
		this.lhashTblSz = NextPrime.getNextPrimeI((int) (this.lhashTblSz));
		long fsz = (long) max * (long) ProgressiveFileByteArrayLongMap.EL;
		SDFSLogger.getLog().info("table setup max=" + max + " maxsz=" + this.maxSz + " _tbs=" + _tbs
				+ " calculated hashtblesz=" + otb + " hashTblSz=" + this.hashTblSz + " filesize=" + fsz);
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
			int sz = NextPrime.getNextPrimeI((int) (this.hashTblSz));
			AtomicLong ep = new AtomicLong();
			AtomicInteger prg = new AtomicInteger();
			AtomicLong rSz = new AtomicLong();
			executor = new ThreadPoolExecutor(Main.writeThreads, Main.writeThreads, 10, TimeUnit.SECONDS, worksQueue,
					new ProcessPriorityThreadFactory(Thread.MIN_PRIORITY), executionHandler);
			ArrayList<DBLoad> al = new ArrayList<DBLoad>();
			for (int i = 0; i < files.length; i++) {
				this.loadEvent.curCt = this.loadEvent.curCt + 1;
				
				// SDFSLogger.getLog().debug("will create byte array of size "
				// + sz + " propsize was " + propsize);
				ProgressiveFileByteArrayLongMap map = null;
				String pth = files[i].getPath();
				String pfx = pth.substring(0, pth.length() - 5);
				map = new ProgressiveFileByteArrayLongMap(pfx, sz);
				DBLoad l = new DBLoad(this,map,prg,ep,rSz,bar);
				executor.execute(l);
				al.add(l);
			}
			executor.shutdown();
			this.endPos = ep.get();
			rsz = rSz.get();
			bar.finish();
			try {
				System.out.print("Waiting for all Maps to load");
				while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
					SDFSLogger.getLog().debug("Awaiting map load completion of threads.");
					System.out.print(".");

				}
				for (DBLoad th : al) {
					if (th.e != null)
						throw th.e;
				}
				System.out.println("done");
			} catch (Exception e1) {
				throw new IOException(e1);
			}

		}

		this.loadEvent.shortMsg = "Loading BloomFilters";

		if (maps.size() != 0 && !LargeBloomFilter.exists(_fs.getParentFile())) {
			lbf = new LargeBloomFilter(_fs.getParentFile(), maxSz, .01, true, true);
			SDFSLogger.getLog().warn("Recreating BloomFilters...");
			this.loadEvent.shortMsg = "Recreating BloomFilters";

			executor = new ThreadPoolExecutor(Main.writeThreads, Main.writeThreads, 10, TimeUnit.SECONDS, worksQueue,
					new ProcessPriorityThreadFactory(Thread.MIN_PRIORITY), executionHandler);
			CommandLineProgressBar bar = new CommandLineProgressBar("ReCreating BloomFilters", maps.size(), System.out);
			Iterator<ProgressiveFileByteArrayLongMap> iter = maps.iterator();
			int i = 0;
			ArrayList<LBFReconstructThread> al = new ArrayList<LBFReconstructThread>();
			while (iter.hasNext()) {
				ProgressiveFileByteArrayLongMap m = iter.next();
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
					SDFSLogger.getLog().debug("Awaiting bloomcreation completion of threads.");
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
			lbf = new LargeBloomFilter(_fs.getParentFile(), maxSz, .01, true, true);
		}
		while (this.activeWriteMaps.size() < AMS) {
			boolean written = false;
			while (!written) {
				String guid = RandomGUID.getGuid();

				File f = new File(fileName + "-" + guid + ".keys");
				if (!f.exists()) {
					ProgressiveFileByteArrayLongMap activeWMap = new ProgressiveFileByteArrayLongMap(
							fileName + "-" + guid, this.hashTblSz);
					activeWMap.setUp();
					activeWMap.activate();
					// activeWMap.initialize();

					this.maps.add(activeWMap);
					written = true;

					this.activeWriteMaps.add(activeWMap);
				}
			}
		}
		SDFSLogger.getLog().info(AMS + " Parallel DBs");
		this.loadEvent.endEvent("Loaded entries " + rsz);
		System.out.println("Loaded entries " + rsz);
		SDFSLogger.getLog().info("Loaded entries " + rsz);
		SDFSLogger.getLog().info("Loading BloomFilters " + rsz);
		this.kSz.set(rsz);
		this.closed = false;
		if (this.hugeTables) {
			if (this.lactiveWMap == null)
				lactiveWMap = this.createWriteMap(this.lhashTblSz);
			DBConsolidator cd = new DBConsolidator(this);
			cdth = new Thread(cd);
			cdth.start();
		}
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
			ProgressiveFileByteArrayLongMap rm = this.getReadMap(cm.getHash());
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
						//lastInsert = System.currentTimeMillis();
					} catch (HashtableFullException e) {
						rec = null;
					} catch (Exception e) {
						throw e;
					}
				}
			} else {
				rec = new InsertRecord(false, rm.get(cm.getHash()));
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
		ProgressiveFileByteArrayLongMap bm = this.getReadMap(cm.getHash());
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
			Iterator<ProgressiveFileByteArrayLongMap> iter = this.maps.iterator();
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
			cdth.interrupt();
			CommandLineProgressBar bar = new CommandLineProgressBar("Closing Hash Tables", this.maps.size(),
					System.out);
			Iterator<ProgressiveFileByteArrayLongMap> iter = this.maps.iterator();
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
			Lock l = gcLock.readLock();
			l.lock();
			try {
				if (!this.runningGC) {
					try {
						this.lbf.save(new File(fileName).getParentFile());
					} catch (Throwable e) {
						SDFSLogger.getLog().warn("unable to save bloomfilter", e);
					}
				}
			} finally {
				l.unlock();
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

	public static void main(String[] args) throws Exception {
		/*
		 * ProgressiveFileBasedCSMap b = new ProgressiveFileBasedCSMap();
		 * b.init(1000000, "/opt/sdfs/hash"); long start =
		 * System.currentTimeMillis(); Random rnd = new Random(); byte[] hash =
		 * null; long val = -33; byte[] hash1 = null; long val1 = -33;
		 * Tiger16HashEngine eng = new Tiger16HashEngine(); for (int i = 0; i <
		 * 60000; i++) { byte[] z = new byte[64]; rnd.nextBytes(z); hash =
		 * eng.getHash(z); val = rnd.nextLong(); if (i == 1) { val1 = val; hash1
		 * = hash; } if (val < 0) val = val * -1; ChunkData cm = new
		 * ChunkData(hash, val); InsertRecord k = b.put(cm); if
		 * (k.getInserted()) System.out.println("Unable to add this " + k);
		 * 
		 * } long end = System.currentTimeMillis(); System.out.println("Took " +
		 * (end - start) / 1000 + " s " + val1); System.out.println("Took " +
		 * (System.currentTimeMillis() - end) / 1000 + " ms at pos " +
		 * b.get(hash1)); b.claimRecords(SDFSEvent.gcInfoEvent("testing 123"));
		 * b.close();
		 */
		long l = ((Integer.MAX_VALUE) / (ProgressiveFileByteArrayLongMap.EL)) * 3;
		System.out.println("length is " + (l * ProgressiveFileByteArrayLongMap.EL));
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
			this.init(maxSz, this.fileName);
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
	
	protected final static class DBLoad implements Runnable {
		final ProgressiveFileBasedCSMap m;
		ProgressiveFileByteArrayLongMap map;
		AtomicInteger prog;
		CommandLineProgressBar bar;
		AtomicLong sz;
		AtomicLong ep;
		Exception e;
		DBLoad(ProgressiveFileBasedCSMap m, ProgressiveFileByteArrayLongMap map,AtomicInteger prog,AtomicLong sz,AtomicLong ep,CommandLineProgressBar bar) {
			this.m = m;
			this.map = map;
			this.prog = prog;
			this.bar = bar;
			this.sz = sz;
			this.ep = ep;
		}

		@Override
		public void run() {
			try {
			long mep = map.setUp();
			synchronized(ep) {
			if (mep > ep.get())
				ep.set(mep);
			}
			m.maps.add(map);
			sz.addAndGet(map.size());
			prog.incrementAndGet();
			bar.update(prog.incrementAndGet());
			if (!map.isFull() && m.activeWriteMaps.size() < AMS && map.maxSize() < m.lhashTblSz) {
				map.activate();
				map.cache();
				m.activeWriteMaps.add(map);
			} else if (!map.isFull() && map.maxSize() == m.lhashTblSz && m.hugeTables) {
				map.activate();
				m.lactiveWMap = map;
				map.cache();
			} else {
				map.inActive();
				map.full = true;
			}
			}catch(Exception e) {
				SDFSLogger.getLog().error("unable to load " + map.toString(),e);
				this.e =e;
			}
			
		}
		
	}

	protected final static class DBCompact implements Runnable {
		final ProgressiveFileBasedCSMap m;
		ProgressiveFileByteArrayLongMap map;
		//private static final long btime = 3 * 1000;

		DBCompact(ProgressiveFileBasedCSMap m, ProgressiveFileByteArrayLongMap map) {
			this.m = m;
			this.map = map;
		}

		@Override
		public void run() {
			try {
				SDFSLogger.getLog().debug(
						"consolidating map " + map.toString() + " sz=" + map.maxSize() + " targetsz=" + m.lhashTblSz);
				this.map.cache();
				map.iterInit();
				int entries = 0;
				Lock l = m.gcLock.readLock();
				l.lock();
				try {
					KVPair p = map.nextKeyValue();
					while (p != null && !m.closed && !map.isClosed()) {
						//long td = System.currentTimeMillis() - m.lastInsert;
						try {
							ProgressiveFileByteArrayLongMap _m = m.getLargeWriteMap();

							_m.put(p.key, p.value);
							ByteArrayWrapper bw = new ByteArrayWrapper(p.key);
							
								if (m.keyLookup.containsKey(bw)) {
									synchronized (m.keyLookup) {
									m.keyLookup.put(bw, _m);
								}
							}
							m.lbf.put(p.key);
							p = map.nextKeyValue();

							entries++;
						} catch (HashtableFullException e) {

						}
						if (m.closed)
							throw new IOException("map closed");

					}
				} finally {
					l.unlock();
				}
				int mapsz = m.maps.size();
				l = m.gcLock.writeLock();
				l.lock();
				try {

					m.maps.remove(map);
					SDFSLogger.getLog()
							.debug("consolidated map " + map.toString() + " sz=" + map.size() + " added " + entries);
				} finally {
					l.unlock();
				}
				map.vanish();
				//m.keyLookup.clear();
				map = null;
				
				mapsz = mapsz - m.maps.size();

			} catch (Exception e) {
				map.compactRunning(false);
				SDFSLogger.getLog().error("unable to consolidate", e);
			}
		}
	}

	private final static class DBConsolidator implements Runnable {
		final ProgressiveFileBasedCSMap m;
		//private static final long btime = 60 * 1000;
		private transient RejectedExecutionHandler executionHandler = new BlockPolicy();
		private transient BlockingQueue<Runnable> worksQueue = new SynchronousQueue<Runnable>();
		private transient ThreadPoolExecutor executor = new ThreadPoolExecutor(1, ProgressiveFileBasedCSMap.AMS, 10,
				TimeUnit.SECONDS, worksQueue, new ProcessPriorityThreadFactory(Thread.MIN_PRIORITY), executionHandler);

		DBConsolidator(ProgressiveFileBasedCSMap m) {
			this.m = m;
		}

		@Override
		public void run() {
			for (;;) {
				if (m.closed)
					break;
				int z = 0;
				try {

					Iterator<ProgressiveFileByteArrayLongMap> iter = m.maps.getLMMap().iterator();
					//long td = System.currentTimeMillis() - m.lastInsert;
					
					while (iter.hasNext() && !m.closed) {
						ProgressiveFileByteArrayLongMap map = iter.next();
						boolean active = false;
						synchronized (m) {
							active = map.isActive();
						}
						if (!map.isCompactig() && map != null && map.maxSize() < m.lhashTblSz && !active && !m.closed
								&& !map.isClosed()) {
							z++;
							map.compactRunning(true);
							DBCompact c = new DBCompact(m, map);
							executor.execute(c);
						}
					}
				} catch (Exception e) {
					SDFSLogger.getLog().error("unable to consolidate", e);
				} finally {
					try {
						if (!m.closed && z>0)
							Thread.sleep(300);
						else if (!m.closed && z==0)
							Thread.sleep(30000);
						else
							break;
					} catch (InterruptedException e) {

					}
				}
			}

		}

	}

	@Override
	public void cache(byte[] key, long pos) throws IOException {
		if (pos == -1) {
			pos = this.get(key);
		}
		if (pos != -1)
			try {

				ChunkData.cacheChunk(key, pos);
			} catch (Exception e) {
				SDFSLogger.getLog().debug("error getting [" + StringUtils.getHexString(key) + "]", e);
			}

	}
}
