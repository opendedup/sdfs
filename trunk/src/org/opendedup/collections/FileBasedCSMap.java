package org.opendedup.collections;

import java.io.File;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.opendedup.collections.BloomFileByteArrayLongMap.KeyBlob;
import org.opendedup.collections.threads.SyncThread;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.hashing.Tiger16HashEngine;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.CommandLineProgressBar;
import org.opendedup.util.NextPrime;
//import org.opendedup.util.OSValidator;
import org.opendedup.util.StringUtils;

import com.google.common.hash.BloomFilter;

public class FileBasedCSMap implements AbstractMap, AbstractHashesMap {
	// RandomAccessFile kRaf = null;
	private long size = 0;
	private final ReentrantLock arlock = new ReentrantLock();
	private final ReentrantLock iolock = new ReentrantLock();
	private String fileName;
	private String origFileName;
	long compactKsz = 0;
	private AbstractShard[] maps = null;
	// private boolean removingChunks = false;
	// private static int freeSlotsLength = 3000000;
	// The amount of memory available for free slots.
	private boolean closed = true;
	private AtomicLong kSz = new AtomicLong(0);
	long ram = 0;
	private long maxSz = 0;
	// TODO change the kBufMazSize so it not reflective to the pageSize
	private byte[] FREE = new byte[HashFunctionPool.hashLength];
	private boolean firstGCRun = true;
	private SyncThread st = null;
	private SDFSEvent loadEvent = SDFSEvent.loadHashDBEvent(
			"Loading Hash Database", Main.mountEvent);
	private long endPos = 0;

	@Override
	public void init(long maxSize, String fileName) throws IOException,
			HashtableFullException {
		maps = new AbstractShard[128];
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

	public AbstractShard getMap(byte[] hash) throws IOException {

		int hashb = hash[2];
		if (hashb < 0) {
			hashb = ((hashb * -1) - 1);
		}
		int hashRoute = hashb;

		AbstractShard m = maps[hashRoute];

		return m;
	}

	@Override
	public long getAllocatedRam() {
		return this.ram;
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
		if (this.isClosed())
			throw new IOException("Hashtable " + this.fileName + " is close");
		SDFSLogger.getLog().info("claiming records");
		SDFSEvent tEvt = SDFSEvent.claimInfoEvent(
				"Claiming Records [" + this.getSize() + "] from ["
						+ this.fileName + "]", evt);
		tEvt.maxCt = this.maps.length;
		long claims = 0;
		for (int i = 0; i < maps.length; i++) {
			tEvt.curCt++;
			try {
				maps[i].iterInit();
				claims = claims + maps[i].claimRecords();
			} catch (Exception e) {
				tEvt.endEvent("Unable to claim records for " + i
						+ " because : [" + e.toString() + "]", SDFSEvent.ERROR);
				SDFSLogger.getLog()
						.error("Unable to claim records for " + i, e);
				throw new IOException(e);
			}
		}
		tEvt.endEvent("claimed [" + claims + "] records");
		SDFSLogger.getLog().info("claimed [" + claims + "] records");
	}

	@Override
	public synchronized long claimRecords(SDFSEvent evt,BloomFilter<KeyBlob> bf) throws IOException {
		if (this.isClosed())
			throw new IOException("Hashtable " + this.fileName + " is close");
		SDFSLogger.getLog().info("Claiming Records [" + this.getSize() + "] from ["
				+ this.fileName + "]");
		SDFSEvent tEvt = SDFSEvent.claimInfoEvent(
				"Claiming Records [" + this.getSize() + "] from ["
						+ this.fileName + "]", evt);
		tEvt.maxCt = this.maps.length;
		long claims = 0;
		for (int i = 0; i < maps.length; i++) {
			tEvt.curCt++;
			try {
				maps[i].iterInit();
				claims = claims + maps[i].claimRecords(bf);
			} catch (Exception e) {
				tEvt.endEvent("Unable to claim records for " + i
						+ " because : [" + e.toString() + "]", SDFSEvent.ERROR);
				SDFSLogger.getLog()
						.error("Unable to claim records for " + i, e);
				throw new IOException(e);
			}
		}
		tEvt.endEvent("removed [" + claims + "] records");
		SDFSLogger.getLog().info("removed [" + claims + "] records");
		return claims;
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
		SDFSLogger.getLog().info("Loading freebits bitset");
		long rsz = 0;
		CommandLineProgressBar bar = new CommandLineProgressBar(
				"Loading Hashes", this.maps.length, System.out);
		this.loadEvent.maxCt = this.maps.length;
		for (int i = 0; i < this.maps.length; i++) {
			this.loadEvent.curCt = this.loadEvent.curCt + 1;
			int sz = NextPrime.getNextPrimeI((int) (size / maps.length));
			// SDFSLogger.getLog().debug("will create byte array of size "
			// + sz + " propsize was " + propsize);
			ram = ram + (sz * (HashFunctionPool.hashLength + 8));
			String fp = this.fileName + "-" + i;
			AbstractShard m = null;
			/*
			if (OSValidator.isWindows())
				m = new FCByteArrayLongMap(fp, sz,
						(short) HashFunctionPool.hashLength);
			else
			*/
				m = new FileByteArrayLongMap(fp, sz,
						(short) HashFunctionPool.hashLength);
			long mep = m.setUp();
			if (mep > endPos)
				endPos = mep;
			maps[i] = m;
			rsz = rsz + m.size();
			bar.update(i);
		}
		bar.finish();
		this.loadEvent.endEvent("Loaded entries " + rsz);
		System.out.println("Loaded entries " + rsz);
		SDFSLogger.getLog().info("Loaded entries " + rsz);
		this.kSz.set(rsz);
		this.closed = false;
		return size;
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
		return this.getMap(key).containsKey(key);
	}

	@Override
	public synchronized long removeRecords(long time, boolean forceRun,
			SDFSEvent evt) throws IOException {
		SDFSLogger.getLog().info(
				"Garbage collection starting for records older than "
						+ new Date(time));
		SDFSEvent tEvt = SDFSEvent
				.claimInfoEvent(
						"Garbage collection starting for records older than "
								+ new Date(time) + " from [" + this.fileName
								+ "]", evt);
		tEvt.maxCt = this.maps.length;
		long rem = 0;
		if (forceRun)
			this.firstGCRun = false;
		if (this.firstGCRun) {
			this.firstGCRun = false;
			tEvt.endEvent(
					"Garbage collection aborted because it is the first run",
					SDFSEvent.WARN);
			throw new IOException(
					"Garbage collection aborted because it is the first run");

		} else {
			if (this.isClosed())
				throw new IOException("Hashtable " + this.fileName
						+ " is close");
			for (int i = 0; i < maps.length; i++) {
				tEvt.curCt++;
				if (maps[i] != null) {
					maps[i].iterInit();
					long fPos = maps[i].removeNextOldRecord(time);
					while (fPos != -1) {
						this.kSz.decrementAndGet();
						rem++;
						fPos = maps[i].removeNextOldRecord(time);
					}
				}
			}
		}
		tEvt.actionCount = rem;
		tEvt.endEvent("Removed [" + rem + "] records. Free slots ["
				+ HCServiceProxy.getFreeBlocks() + "]");
		SDFSLogger.getLog().info(
				"Removed [" + rem + "] records. Free slots ["
						+ HCServiceProxy.getFreeBlocks() + "]");
		return rem;
	}

	@Override
	public boolean put(ChunkData cm) throws IOException, HashtableFullException {
		if (this.isClosed())
			throw new HashtableFullException("Hashtable " + this.fileName
					+ " is close");
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
		boolean added = false;
		added = this.put(cm, true);
		return added;
	}

	@Override
	public boolean put(ChunkData cm, boolean persist) throws IOException,
			HashtableFullException {
		// persist = false;
		if (this.isClosed())
			throw new HashtableFullException("Hashtable " + this.fileName
					+ " is close");
		if (kSz.get() >= this.maxSz)
			throw new HashtableFullException("maximum sized reached");
		boolean added = false;
		// if (persist)
		// this.flushFullBuffer();
		if (persist) {
			if (!cm.recoverd) {
				cm.persistData(true);
			}
			added = this.getMap(cm.getHash()).put(cm.getHash(), cm.getcPos());
			if (added) {
				this.kSz.incrementAndGet();
			} else {
				cm.setmDeleteDuplicate(true);
			}
		} else {
			added = this.getMap(cm.getHash()).put(cm.getHash(), cm.getcPos());
		}
		return added;
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
		this.arlock.lock();
		try {
			boolean added = false;
			if (!this.isClaimed(cm)) {
				cm.persistData(true);
				added = this.getMap(cm.getHash()).update(cm.getHash(),
						cm.getcPos());
			}
			if (added) {
				this.compactKsz++;
			}
			return added;
		} catch (KeyNotFoundException e) {
			return false;
		} finally {
			this.arlock.unlock();

		}
	}

	public boolean isClaimed(ChunkData cm) throws KeyNotFoundException,
			IOException {
		return this.getMap(cm.getHash()).isClaimed(cm.getHash());
	}

	@Override
	public long get(byte[] key) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		return this.getMap(key).get(key);
	}

	@Override
	public byte[] getData(byte[] key) throws IOException {
		if (this.isClosed())
			throw new IOException("Hashtable " + this.fileName + " is close");
		long ps = this.get(key);
		if (ps != -1) {
			return ChunkData.getChunk(key, ps);
		} else {
			SDFSLogger.getLog().info(
					"found no data for key [" + StringUtils.getHexString(key)
							+ "]");
			return null;
		}

	}

	@Override
	public boolean remove(ChunkData cm) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		// this.flushFullBuffer();
		try {
			if (cm.getHash().length == 0)
				return true;
			if (!this.getMap(cm.getHash()).remove(cm.getHash())) {
				return false;
			} else {
				cm.setmDelete(true);
				this.arlock.lock();
				if (this.isClosed()) {
					throw new IOException("hashtable [" + this.fileName
							+ "] is close");
				}
				try {
					this.kSz.decrementAndGet();
				} catch (Exception e) {
				} finally {
					this.arlock.unlock();
				}

				return true;
			}
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error getting record", e);
			return false;
		}
	}

	private ReentrantLock syncLock = new ReentrantLock();

	@Override
	public void sync() throws IOException {
		syncLock.lock();
		try {
			if (this.isClosed()) {
				throw new IOException("hashtable [" + this.fileName
						+ "] is close");
			}
			for (int i = 0; i < this.maps.length; i++) {
				try {
					this.maps[i].sync();
				} catch (IOException e) {
					SDFSLogger.getLog().warn("Unable to sync table " + i, e);
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
		this.arlock.lock();
		this.iolock.lock();
		this.syncLock.lock();
		try {
			this.closed = true;
			try {
				st.close();
			} catch (Exception e) {
			}
			for (int i = 0; i < this.maps.length; i++) {
				this.maps[i].close();
				this.maps[i] = null;
			}
		} finally {
			this.arlock.unlock();
			this.iolock.unlock();
			this.syncLock.unlock();
			SDFSLogger.getLog()
					.info("Hashtable [" + this.fileName + "] closed");
		}
	}

	@Override
	public void vanish() throws IOException {
		// TODO Auto-generated method stub

	}

	public static void main(String[] args) throws Exception {
		FileBasedCSMap b = new FileBasedCSMap();
		b.init(1000000, "/opt/sdfs/hash");
		long start = System.currentTimeMillis();
		Random rnd = new Random();
		byte[] hash = null;
		long val = -33;
		byte[] hash1 = null;
		long val1 = -33;
		Tiger16HashEngine eng = new Tiger16HashEngine();
		for (int i = 0; i < 60000; i++) {
			byte[] z = new byte[64];
			rnd.nextBytes(z);
			hash = eng.getHash(z);
			val = rnd.nextLong();
			if (i == 1) {
				val1 = val;
				hash1 = hash;
			}
			if (val < 0)
				val = val * -1;
			ChunkData cm = new ChunkData(hash, val);
			boolean k = b.put(cm);
			if (k == false)
				System.out.println("Unable to add this " + k);

		}
		long end = System.currentTimeMillis();
		System.out.println("Took " + (end - start) / 1000 + " s " + val1);
		System.out.println("Took " + (System.currentTimeMillis() - end) / 1000
				+ " ms at pos " + b.get(hash1));
		b.claimRecords(SDFSEvent.gcInfoEvent("testing 123"));
		b.removeRecords(10, true, SDFSEvent.gcInfoEvent("testing 123"));
		b.close();

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
		FileUtils
				.copyDirectory(new File(parent), new File(parent + ".compact"));

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
		SDFSLogger.getLog().info(
				"Deleted " + new File(this.origFileName).getParent());
		new File(this.fileName).getParentFile().renameTo(
				new File(this.origFileName).getParentFile());
		SDFSLogger.getLog().info(
				"moved " + new File(this.fileName).getParent() + " to "
						+ new File(this.origFileName).getParent());
		FileUtils.deleteDirectory(new File(this.fileName).getParentFile());
		SDFSLogger.getLog().info(
				"deleted " + new File(this.fileName).getParent());

	}

	@Override
	public void rollbackCompact() throws IOException {
		FileUtils.deleteDirectory(new File(this.fileName));

	}
}
