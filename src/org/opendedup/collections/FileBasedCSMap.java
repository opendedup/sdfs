package org.opendedup.collections;

import java.io.File;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.collections.AbstractHashesMap;
import org.opendedup.collections.AbstractMap;
import org.opendedup.collections.HashtableFullException;
import org.opendedup.collections.threads.SyncThread;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.hashing.Tiger16HashEngine;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.servers.HashChunkService;
import org.opendedup.util.NextPrime;
import org.opendedup.util.SDFSLogger;
import org.opendedup.util.StringUtils;

public class FileBasedCSMap implements AbstractMap, AbstractHashesMap {
	// RandomAccessFile kRaf = null;
	private long size = 0;
	private final ReentrantLock arlock = new ReentrantLock();
	private final ReentrantLock iolock = new ReentrantLock();
	private String fileName;
	private FileByteArrayLongMap[] maps = null;
	// private boolean removingChunks = false;
	// private static int freeSlotsLength = 3000000;
	// The amount of memory available for free slots.
	private boolean closed = true;
	long kSz = 0;
	long ram = 0;
	private long maxSz = 0;
	// TODO change the kBufMazSize so it not reflective to the pageSize
	private BitSet freeSlots = null;
	private byte[] FREE = new byte[HashFunctionPool.hashLength];
	private boolean firstGCRun = true;

	public void init(long maxSize, String fileName) throws IOException,
			HashtableFullException {
		
		maps = new FileByteArrayLongMap[256];
		this.size = (long) (maxSize);
		this.maxSz = maxSize;
		this.fileName = fileName;
		try {
			this.setUp();
		} catch (Exception e) {
			throw new IOException(e);
		}
		this.closed = false;
		new SyncThread(this);
	}

	public FileByteArrayLongMap getMap(byte[] hash) throws IOException {
		int hashb = 0;
		if (Main.compressedIndex) {
			hashb = ByteBuffer.wrap(hash).getShort(hash.length - 2);
			if (hashb < 0) {
				hashb = ((hashb * -1) + 32766);
			}
		} else {
			hashb = hash[2];
			if (hashb < 0) {
				hashb = ((hashb * -1) -1);
			}

		}
		int hashRoute = hashb;

		FileByteArrayLongMap m = maps[hashRoute];
		return m;
	}

	public long getAllocatedRam() {
		return this.ram;
	}

	public boolean isClosed() {
		return this.closed;
	}

	public long getSize() {
		return this.kSz;
	}

	public long getUsedSize() {

		return kSz * Main.CHUNK_LENGTH;
	}

	public long getMaxSize() {
		return this.size;
	}

	public synchronized void claimRecords() throws IOException {
		if (this.isClosed())
			throw new IOException("Hashtable " + this.fileName + " is close");
		SDFSLogger.getLog().info("claiming records");
		long claims = 0;
		for (int i = 0; i < maps.length; i++) {
			try {
				maps[i].iterInit();
				claims = claims + maps[i].claimRecords();
			} catch (Exception e) {
				SDFSLogger.getLog()
						.error("Unable to claim records for " + i, e);
				throw new IOException(e);
			}
		}
		SDFSLogger.getLog().info("claimed [" + claims + "] records");
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
		long endPos = 0;
		SDFSLogger.getLog().info("Loading freebits bitset");
		File bsf = new File(this.fileName + "freebit.map");
		if(!bsf.exists()) {
			SDFSLogger.getLog().debug("Looks like a new HashStore");
			this.freeSlots = new BitSet();
		} else {
			SDFSLogger.getLog().info("Loading freeslots from " + bsf.getPath());
			try {
				FileInputStream fin = new FileInputStream(bsf);
				ObjectInputStream oon = new ObjectInputStream(fin);
				this.freeSlots = (BitSet)oon.readObject();
				oon.close();
				fin.close();
				bsf.delete();
			}catch(Exception e) {
				SDFSLogger.getLog().error("Unable to load bitset from " + bsf.getPath(),e);
			}
			SDFSLogger.getLog().info("Loaded [" + this.freeSlots.cardinality() + "] free slots");
		}
		System.out.println("Loading Hashtable Entries");
		long rsz = 0;
		for (int i = 0; i < this.maps.length; i++) {
			int sz = NextPrime.getNextPrimeI((int) (size / maps.length));
			// SDFSLogger.getLog().debug("will create byte array of size "
			// + sz + " propsize was " + propsize);
			ram = ram + (sz * (HashFunctionPool.hashLength + 8));
			String fp = this.fileName + "-" + i;
			FileByteArrayLongMap m = new FileByteArrayLongMap(fp, sz,
					(short)HashFunctionPool.hashLength);
			long mep = m.setUp();
			if(mep > endPos)
				endPos = mep;
			maps[i] = m;
			rsz = rsz + m.size();
		}
		System.out.println("");
		System.out.println(" Loaded " + rsz);
		SDFSLogger.getLog().info("Loaded entries " + rsz);
		System.out.println();
		HashChunkService.getChuckStore().setSize(endPos + HashFunctionPool.hashLength);
		return size;
	}

	/**
	 * Searches the set for <tt>obj</tt>
	 * 
	 * @param obj
	 *            an <code>Object</code> value
	 * @return a <code>boolean</code> value
	 * @throws IOException
	 */
	public boolean containsKey(byte[] key) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		return this.getMap(key).containsKey(key);
	}

	public int getFreeBlocks() {
		return this.freeSlots.cardinality();
	}

	public synchronized long removeRecords(long time, boolean forceRun)
			throws IOException {
		SDFSLogger.getLog().info(
				"Garbage collection starting for records older than "
						+ new Date(time));
		long rem = 0;
		if (forceRun)
			this.firstGCRun = false;
		if (this.firstGCRun) {
			this.firstGCRun = false;
			throw new IOException(
					"Garbage collection aborted because it is the first run");
		} else {
			if (this.isClosed())
				throw new IOException("Hashtable " + this.fileName
						+ " is close");
			for (int i = 0; i < maps.length; i++) {
				if (maps[i] != null) {
					maps[i].iterInit();
					long fPos = maps[i].removeNextOldRecord(time);
					while (fPos >= 0) {
						this.addFreeSlot(fPos);
						rem++;
						fPos = maps[i].removeNextOldRecord(time);
					}
				}
			}
		}
		SDFSLogger.getLog().info(
				"Removed [" + rem + "] records. Free slots ["
						+ this.freeSlots.cardinality() + "]");
		return rem;
	}

	public boolean put(ChunkData cm) throws IOException, HashtableFullException {
		if (this.isClosed())
			throw new HashtableFullException("Hashtable " + this.fileName
					+ " is close");
		if (this.kSz >= this.maxSz)
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

	private ReentrantLock fslock = new ReentrantLock();

	protected long getFreeSlot() {
		fslock.lock();
		try {
			int slot = this.freeSlots.nextSetBit(0);
			if (slot != -1) {
				this.freeSlots.clear(slot);
				return (long) ((long) slot * (long) Main.CHUNK_LENGTH);
			} else
				return slot;
		} finally {
			fslock.unlock();
		}
	}

	private void addFreeSlot(long position) {
		this.fslock.lock();
		try {
			int pos = (int) (position / Main.CHUNK_LENGTH);
			if (pos >= 0)
				this.freeSlots.set(pos);
			else if (pos < 0) {
				SDFSLogger.getLog().info("Position is less than 0 " + pos);
			}
		} finally {
			this.fslock.unlock();
		}
	}

	public boolean put(ChunkData cm, boolean persist) throws IOException,
			HashtableFullException {
		// persist = false;
		if (this.isClosed())
			throw new HashtableFullException("Hashtable " + this.fileName
					+ " is close");
		if (kSz >= this.maxSz)
			throw new HashtableFullException("maximum sized reached");
		boolean added = false;
		// if (persist)
		// this.flushFullBuffer();
		if (persist) {
			cm.setcPos(this.getFreeSlot());
			cm.persistData(true);
			added = this.getMap(cm.getHash()).put(cm.getHash(), cm.getcPos());
			if (added) {
				this.arlock.lock();
				this.kSz++;
				this.arlock.unlock();
			} else {
				this.addFreeSlot(cm.getcPos());
				cm = null;
			}
		} else {
			added = this.getMap(cm.getHash()).put(cm.getHash(), cm.getcPos());
		}
		return added;
	}

	public boolean update(ChunkData cm) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		return false;

	}

	public long get(byte[] key) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		return this.getMap(key).get(key);

	}

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
					this.addFreeSlot(cm.getcPos());
					this.kSz--;
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
				}catch(	IOException e) {
					SDFSLogger.getLog().warn("Unable to sync table " + i,e);
				}
			}
			// this.flushBuffer(true);
			// this.kRaf.getFD().sync();
		} finally {
			syncLock.unlock();
		}
	}

	public void close() {
		this.arlock.lock();
		this.iolock.lock();
		this.closed = true;
		for (int i = 0; i < this.maps.length; i++) {
			this.maps[i].close();
		}
		try {
			File outFile = new File(this.fileName + "freebit.map");
		    FileOutputStream fos = new FileOutputStream(outFile);
		    ObjectOutputStream oos = new ObjectOutputStream(fos);
		    oos.writeObject(this.freeSlots);
		    oos.flush();
		    fos.flush();
		    fos.close();
		}catch(Exception e) {
			SDFSLogger.getLog().error("unable to serialize free bits bitmap " + this.fileName + "freebit.map",e );
		}
		this.arlock.unlock();
		this.iolock.unlock();
		SDFSLogger.getLog().info("Hashtable [" + this.fileName + "] closed");
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
		b.claimRecords();
		b.removeRecords(10, true);
		b.close();

	}

	@Override
	public void initCompact() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commitCompact(boolean force) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rollbackCompact() throws IOException {
		// TODO Auto-generated method stub
		
	}
}
