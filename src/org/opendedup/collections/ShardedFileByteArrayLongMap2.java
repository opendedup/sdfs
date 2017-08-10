package org.opendedup.collections;

import java.io.File;




import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.opendedup.collections.AbstractShard;
import org.opendedup.collections.HashtableFullException;
import org.opendedup.collections.KeyNotFoundException;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.hashing.LargeBloomFilter;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.util.NextPrime;
import org.opendedup.utils.hashing.FileBasedBloomFilter;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import sun.nio.ch.DirectBuffer;

@SuppressWarnings("restriction")
public class ShardedFileByteArrayLongMap2
		implements Runnable, AbstractShard, Serializable, Comparable<ShardedFileByteArrayLongMap2> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	// transient MappedByteBuffer keys = null;
	transient private int size = 0;
	transient private int maxSz = 0;
	transient private double loadFactor = .75;
	transient private String path = null;
	transient private FileChannel kFC = null;
	transient protected static final int EL = HashFunctionPool.hashLength + 8 + 8;
	transient protected static final int ZL = HashFunctionPool.hashLength + 8;
	transient private static final int VP = HashFunctionPool.hashLength;
	transient private ReentrantReadWriteLock hashlock = new ReentrantReadWriteLock();
	transient public static byte[] FREE = new byte[HashFunctionPool.hashLength];
	transient public static byte[] REMOVED = new byte[HashFunctionPool.hashLength];
	transient private Iterator<Shard> iter = null;
	transient private Shard currentIter = null;
	transient private boolean closed = false;
	private int numshards = 32;
	private int numdiv = 256 / numshards;
	transient private ArrayList<BitSet> claims = new ArrayList<BitSet>(16);
	transient private ArrayList<BitSet> removed = new ArrayList<BitSet>(16);
	transient private ArrayList<BitSet> mapped = new ArrayList<BitSet>(16);
	transient private FileBasedBloomFilter<KeyBlob> bf = null;
	transient private AtomicInteger sz = new AtomicInteger(0);
	transient RandomAccessFile kRaf = null;
	transient boolean full = false;
	transient private boolean compacting = false;
	transient private boolean active = false;
	private transient AtomicLong lastFound = new AtomicLong();
	//private AtomicLong nextCached = new AtomicLong();
	//private static final long mixCacheTM = 60 * 60 * 1000;
	private boolean cacheRunning = false;
	int minSz = 10000;

	private ArrayList<Shard> shards = new ArrayList<Shard>(numshards);

	static {
		FREE = new byte[HashFunctionPool.hashLength];
		REMOVED = new byte[HashFunctionPool.hashLength];
		Arrays.fill(FREE, (byte) 0);
		Arrays.fill(REMOVED, (byte) 1);
	}

	protected ShardedFileByteArrayLongMap2(String path, int size) throws IOException {
		this.size = size;
		this.path = path;
		if (this.size < 1_000_000) {
			this.numshards = 4;

		} else if (this.size < 10_000_000) {
			this.numshards = 8;

		} else if (this.size < 40_000_000) {
			this.numshards = 16;

		} else if (this.size < 100_000_000) {
			this.numshards = 32;

		} else
			this.numshards = 64;

		numdiv = 256 / numshards;
	}

	@Override
	public void compactRunning(boolean running) {
		this.compacting = running;
	}

	@Override
	public long getLastAccess() {
		return this.lastFound.get();
	}

	public boolean equals(ShardedFileByteArrayLongMap2 m) {
		if (m == null)
			return false;
		if (m.path == null)
			return false;
		if (m.path.equals(this.path))
			return true;
		else
			return false;

	}

	@Override
	public boolean isCompactig() {
		return this.compacting;
	}

	@Override
	public void inActive() {
		synchronized(this) {
			this.active = false;
		}
	}

	@Override
	public void activate() {
		synchronized(this) {
		this.lastFound.set(System.currentTimeMillis());
		this.active = true;
		}
	}

	@Override
	public boolean isActive() {
		synchronized(this) {
			return this.active;
		}
	}

	@Override
	public synchronized void cache() {
		/*
		if (this.nextCached.get() < System.currentTimeMillis() && !this.cacheRunning) {
			synchronized (this.nextCached) {
				if (!this.cacheRunning) {
					this.cacheRunning = true;
					Thread th = new Thread(this);
					th.start();
				}
			}
		}
		*/
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#iterInit()
	 */
	@Override
	public synchronized void iterInit() {
		// this.iterPos = 0;
		this.iter = this.shards.iterator();
		this.currentIter = null;
	}

	@Override
	public boolean isFull() {
		synchronized(this) {
		return full;
		}
	}

	@Override
	public boolean isMaxed() {
		synchronized(this) {
		double nms = (double) maxSz + ((double) maxSz * .1);
		return this.sz.get() >= nms;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#nextKey()
	 */
	@Override
	public byte[] nextKey() throws IOException {
		return this._nextKey(false);
	}

	private byte[] _nextKey(boolean recreate) throws IOException {
		for (;;) {

			if (this.currentIter == null) {
				if (iter.hasNext()) {
					this.currentIter = iter.next();
					this.currentIter.iterInit();
				} else {
					return null;
				}
			}
			byte[] key = this.currentIter.nextKey(recreate);
			if (key != null) {
				this.bf.put(key);
				return key;
			} else
				currentIter = null;

		}
	}

	@Override
	public KVPair nextKeyValue() throws IOException {
		for (;;) {

			if (this.currentIter == null) {
				if (iter.hasNext()) {
					this.currentIter = iter.next();
					this.currentIter.iterInit();
				} else {
					return null;
				}
			}
			
			KVPair key = this.currentIter.nextKeyValue();
			
			if (key != null) {
				this.bf.put(key.key);
				return key;
			} else {
				currentIter = null;
			}
		}

	}

	@Override
	public void clearRefMap() throws IOException {
		Lock l = this.hashlock.writeLock();
		l.lock();
		try {
			ByteBuffer bk = ByteBuffer.allocateDirect(8);
			bk.putLong(0);
				kFC.position(0);
				while (!this.isClosed() && (kFC.position() + EL) < kFC.size() && !this.closed) {
					kFC.position(kFC.position() + ZL);
					bk.position(0);
					kFC.write(bk);
				}
		} finally {
			l.unlock();
		}
	}

	private void recreateMap(int sz) throws IOException {
		Lock l = this.hashlock.writeLock();
		l.lock();
		try {
			bf = FileBasedBloomFilter.create(kbFunnel, size, .04, new
			File(path + ".nbf").getPath(), true);
			this.iterInit();
			byte[] key = this._nextKey(true);
			while (key != null) {
				key = this._nextKey(true);
				// this.bf.put(key);
			}
			long rsz = 0;
			for (BitSet s : this.mapped) {
				rsz += s.cardinality();
			}
			SDFSLogger.getLog().warn("Recovered Hashmap " + this.path + " entries = " + rsz);
		} finally {
			l.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#setUp()
	 */

	boolean setup = false;
	boolean newInstance = false;

	@Override
	public synchronized long setUp() throws IOException {
		if (!setup) {
			File posFile = new File(path + ".keys");
			newInstance = !posFile.exists();
			if (posFile.exists()) {
				long _sz = (posFile.length()) / (long) (EL);
				if (_sz != size) {

					SDFSLogger.getLog().warn("Resetting size of hashtable to [" + _sz + "] instead of [" + size + "]");
					this.size = (int) _sz;
					// System.out.println("nsz="+this.size +" nfs=" +
					// posFile.length());
				}
			}

			// SDFSLogger.getLog().info("sz=" + size + " maxSz=" + this.maxSz);

			SDFSLogger.getLog().info("set table file size to size " + posFile.length());

			boolean closedCorrectly = true;

			int partSize = size / numshards;
			// System.out.println("size="+size);
			// int lastPacket = shardSize - ((numshards-1) * partSize);

			long nsz = 0;
			if (newInstance) {
				partSize = NextPrime.getNextPrimeI(partSize);
				if (partSize < this.minSz) {
					partSize = NextPrime.getNextPrimeI(minSz);
				}
				// lastPacket = NextPrime.getNextPrimeI((int)lastPacket);
				size = (partSize * numshards);
				
				nsz = (long) size * (long) EL;
				// System.out.println("nsize="+size + "nsz=" + nsz);
			} else {
				nsz = posFile.length();
			}
			// SDFSLogger.getLog().info("partsize=" + partSize);
			this.maxSz = (int) (size * loadFactor);
			// SDFSLogger.getLog().info("set table to size " + nsz);
			kRaf = new RandomAccessFile(path + ".keys", "rw");
			this.kFC = FileChannel.open(new File(path + ".keys").toPath(), StandardOpenOption.CREATE,
					StandardOpenOption.WRITE, StandardOpenOption.SPARSE, StandardOpenOption.READ);
			
			if (newInstance) {
				kRaf.setLength(nsz);
				for (int i = 0; i < (numshards); i++) {
					BitSet mp = new BitSet(partSize);
					BitSet rm = new BitSet(partSize);
					BitSet cl = new BitSet(partSize);
					this.mapped.add(i, mp);
					this.removed.add(i, rm);
					this.claims.add(i, cl);
					// long pos = (long) i * (long) partSize * (long) EL;
					bf = FileBasedBloomFilter.create(kbFunnel, size, .04, new
							File(path + ".nbf").getPath(), true);
					Shard sh = new Shard(this, i * partSize, partSize, mp, cl, rm);
					this.shards.add(i, sh);
				}
				// bf = FileBasedBloomFilter.create(kbFunnel, size, .01, new
				// File(path + ".nbf").getPath(), true);
				this.full = false;
				this.initialize();
			} else {
				File f = new File(path + ".bpos");
				if (!f.exists()) {
					closedCorrectly = false;
					SDFSLogger.getLog().warn("bpos does not exist");
				} else {
					try {
						RandomAccessFile _bpos = new RandomAccessFile(path + ".bpos", "rw");
						_bpos.seek(0);
						this.full = _bpos.readBoolean();
						try {
							this.lastFound.set(_bpos.readLong());
						} catch (Exception e) {

						}
						this.lastFound.set(System.currentTimeMillis());
						_bpos.close();
						f.delete();
					} catch (Exception e) {
						SDFSLogger.getLog().warn("bpos load error", e);
						closedCorrectly = false;
					}
				}
				f = new File(path + ".vmp");
				if (!f.exists()) {
					closedCorrectly = false;
					SDFSLogger.getLog().warn("vmp does not exist for " + this.path);
				} else {
					try {
						FileInputStream fin = new FileInputStream(f);
						ObjectInputStream oon = new ObjectInputStream(fin);
						for (int i = 0; i < numshards; i++) {
							BitSet mp = (BitSet) oon.readObject();
							this.mapped.add(i, mp);
						}
						oon.close();
					} catch (Exception e) {

						SDFSLogger.getLog().warn("vmp load error", e);
						closedCorrectly = false;
					}
					f.delete();
				}
				f = new File(path + ".vrp");
				if (!f.exists()) {
					closedCorrectly = false;
					SDFSLogger.getLog().warn("vrp does not exist for " + this.path);
				} else {
					try {
						FileInputStream fin = new FileInputStream(f);
						ObjectInputStream oon = new ObjectInputStream(fin);
						for (int i = 0; i < numshards; i++) {
							BitSet mp = (BitSet) oon.readObject();
							this.removed.add(i, mp);
						}
						oon.close();
					} catch (Exception e) {
						SDFSLogger.getLog().warn("vrp load error", e);
						closedCorrectly = false;
					}
					f.delete();
				}
				f = new File(path + ".nbf");
				if (!f.exists()) {
					closedCorrectly = false;
					SDFSLogger.getLog().warn("bf does not exist");
				} else {
					try {
						bf = FileBasedBloomFilter.create(kbFunnel, size, .04,new File(path + ".nbf").getPath(),true);
					} catch (Exception e) {
						SDFSLogger.getLog().warn("bf load error", e);
						closedCorrectly = false;
					}
				}
				for (int i = 0; i < (numshards); i++) {
					BitSet cl = new BitSet(partSize);
					this.claims.add(i, cl);
				}

				int cp = 0;
				if (!closedCorrectly) {
					this.mapped = new ArrayList<BitSet>();
					this.removed = new ArrayList<BitSet>();
					this.claims = new ArrayList<BitSet>();
					for (int i = 0; i < (numshards); i++) {
						BitSet mp = new BitSet(partSize);
						BitSet rm = new BitSet(partSize);
						BitSet cl = new BitSet(partSize);
						this.mapped.add(i, mp);
						this.removed.add(i, rm);
						this.claims.add(i, cl);
					}
				}
				for (int i = 0; i < numshards; i++) {
					Shard sh = new Shard(this, cp, partSize, mapped.get(i), claims.get(i), removed.get(i));
					shards.add(i, sh);
					cp += partSize;
				}

				if (!closedCorrectly) {
					this.recreateMap(partSize);
				}
			}
			if (this.lastFound.get() == 0)
				this.lastFound.set(new File(path + ".keys").lastModified());

			for (BitSet s : this.mapped) {
				sz.addAndGet(s.cardinality());
			}
			// double pfull = (double) this.sz.get() / (double) size;
			// SDFSLogger.getLog().info("Percentage full=" + pfull + " full=" +
			// this.full);
			SDFSLogger.getLog().info("opened hashtable " + path + " size = " + this.sz.get());
			this.setup = true;
			return this.sz.get();
		} else {
			throw new IOException("already setup");
		}
	}

	private Shard getMap(byte[] hash) {

		int hashb = hash[1];
		if (hashb < 0) {
			hashb = ((hashb * -1) + 127);
		}
		Shard m = this.shards.get(hashb / numdiv);
		return m;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#containsKey(byte[])
	 */
	@Override
	public boolean containsKey(byte[] key) {

		try {
			if (!bf.mightContain(key)) {
				return false;
			}
			Shard sh = this.getMap(key);
			synchronized (sh) {
				return sh.containsKey(key);
			}
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error getting record", e);
			return false;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#isClaimed(byte[])
	 */
	@Override
	public boolean isClaimed(byte[] key) throws KeyNotFoundException, IOException {
		/*
		 * if (!bf.mightContain(key)) throw new KeyNotFoundException();
		 */
		Shard sh = this.getMap(key);
		synchronized (sh) {
			if (sh.isClaimed(key)) {
				this.lastFound.set(System.currentTimeMillis());
				return true;
			} else {
				return false;
			}

		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#update(byte[], long)
	 */
	@Override
	public boolean update(byte[] key, long value) throws IOException {
		/*
		 * if (!bf.mightContain(key)) return false;
		 */
		Shard sh = this.getMap(key);
		synchronized (sh) {
			if (sh.update(key, value)) {
				this.lastFound.set(System.currentTimeMillis());
				return true;
			} else {
				return false;
			}

		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#remove(byte[])
	 */

	@Override
	public boolean remove(byte[] key) throws IOException {
		/*
		 * if (!bf.mightContain(key)) return false;
		 */
		Shard sh = this.getMap(key);
		synchronized (sh) {
			if (sh.remove(key)) {
				this.lastFound.set(System.currentTimeMillis());
				this.sz.decrementAndGet();
				synchronized(this) {
				this.full = false;
				}
				return true;
			} else {
				return false;
			}

		}
	}

	// transient ByteBuffer zlb = ByteBuffer.wrap(new byte[EL]);

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#put(byte[], long)
	 */
	@Override
	public InsertRecord put(ChunkData cm) throws HashtableFullException, IOException, MapClosedException {
		this.hashlock.readLock().lock();
		try {
			if (this.isClosed())
				throw new MapClosedException();
			byte[] key = cm.getHash();
			synchronized(this) {
			if (!this.active || this.full || this.sz.get() >= maxSz) {
				this.full = true;
				this.active = false;
				SDFSLogger.getLog().warn("entries is greater than or equal to the maximum number of entries. You need to expand"
								+ "the volume or DSE allocation size");
				throw new HashtableFullException(
						"entries is greater than or equal to the maximum number of entries. You need to expand"
								+ "the volume or DSE allocation size");
			}
			}
			Shard sh = this.getMap(key);
			InsertRecord r = null;
			synchronized (sh) {
				r = sh.put(cm.getHash(), cm.getcPos(), cm.references, true);
			}
			if (r.getInserted()) {
				this.bf.put(cm.getHash());
				this.sz.incrementAndGet();
				/*
				 * synchronized(bf) { this.bf.put(key); }
				 */
			}
			return r;
		} catch (HashtableFullException e) {
			synchronized(this) {
			this.full = true;
			}
			throw e;
		} finally {
			this.hashlock.readLock().unlock();
		}

	}

	@Override
	public InsertRecord put(byte[] key, long value) throws HashtableFullException, IOException {
		this.hashlock.readLock().lock();
		try {
			synchronized(this) {
			if (!this.active || this.full || this.sz.get() >= maxSz) {
				this.full = true;
				this.active = false;
				throw new HashtableFullException(
						"entries is greater than or equal to the maximum number of entries. You need to expand"
								+ "the volume or DSE allocation size");
			}
			}
			Shard sh = this.getMap(key);
			InsertRecord r = null;
			synchronized (sh) {
				r = sh.put(key, value, -1, true);
			}
			if (r.getInserted()) {
				this.bf.put(key);
				this.sz.incrementAndGet();
				/*
				 * synchronized(bf) { this.bf.put(key); }
				 */
			}
			return r;

		} catch (HashtableFullException e) {
			synchronized(this) {
			this.full = true;
			}
			throw e;
		} finally {
			this.hashlock.readLock().unlock();
		}
	}

	@Override
	public InsertRecord put(byte[] key, long value, long claims) throws HashtableFullException, IOException {
		this.hashlock.readLock().lock();
		try {
			synchronized(this) {
			if (!this.active || this.full || this.sz.get() >= maxSz) {
				this.full = true;
				this.active = false;
				throw new HashtableFullException(
						"entries is greater than or equal to the maximum number of entries. You need to expand"
								+ "the volume or DSE allocation size");
			}
			}
			Shard sh = this.getMap(key);
			InsertRecord r = null;
			synchronized (sh) {
				r = sh.put(key, value, claims, true);
			}
			if (r.getInserted()) {
				this.sz.incrementAndGet();
				this.bf.put(key);
				/*
				 * synchronized(bf) { this.bf.put(key); }
				 */
			}
			return r;

		} catch (HashtableFullException e) {
			synchronized(this) {
			this.full = true;
			}
			throw e;
		} finally {
			this.hashlock.readLock().unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#getEntries()
	 */
	@Override
	public int getEntries() {
		return this.sz.get();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#get(byte[])
	 */
	@Override
	public long get(byte[] key) throws MapClosedException {
		return this.get(key, true);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#get(byte[], boolean)
	 */

	@Override
	public long get(byte[] key, boolean claim) throws MapClosedException {
		this.hashlock.readLock().lock();
		try {
			if(!this.bf.mightContain(key))
				return -1;
			if (this.isClosed())
				throw new MapClosedException();
			Shard sh = this.getMap(key);
			synchronized (sh) {
				return sh.get(key, claim);
			}
		} finally {
			this.hashlock.readLock().unlock();
		}

	}

	@Override
	public boolean claim(byte[] key, long val, long ct) throws MapClosedException {
		this.hashlock.readLock().lock();
		try {
			if (this.isClosed())
				throw new MapClosedException();
			Shard sh = this.getMap(key);
			synchronized (sh) {
				return sh.claimKey(key, val, ct);
			}
		} finally {
			this.hashlock.readLock().unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#size()
	 */
	@Override
	public int size() {
		return this.sz.get();
	}

	@Override
	public int avail() {
		return size - this.sz.get();
	}

	@Override
	public int maxSize() {
		return this.size;
	}

	public String toString() {
		return this.path;
	}

	@Override
	public boolean isClosed() {
		Lock l = this.hashlock.readLock();
		l.lock();
		try {
			return this.closed;
		} finally {
			l.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#close()
	 */
	@Override
	public void close() {
		Lock l = this.hashlock.writeLock();
		l.lock();
		long _sz = 0;
		try {
			this.closed = true;
			for (Shard s : shards) {
				synchronized (s) {
					s.close();
				}
			}

			try {
				this.kFC.close();

			} catch (Exception e) {
				SDFSLogger.getLog().error("error closing", e);
			}

			try {
				this.kRaf.close();
			} catch (Exception e) {
				// SDFSLogger.getLog().error("error closing", e);
			}

			try {
				File f = new File(path + ".vmp");
				FileOutputStream fout = new FileOutputStream(f);
				ObjectOutputStream oon = new ObjectOutputStream(fout);
				for (int i = 0; i < numshards; i++) {
					oon.writeObject(mapped.get(i));
					_sz += mapped.get(i).cardinality();
				}

				oon.flush();
				fout.getFD().sync();
				oon.close();
				fout.flush();

				fout.close();
			} catch (Exception e) {
				SDFSLogger.getLog().warn("error closing", e);
			}
			if (this.bf != null) {
				try {
					bf.close();
				} catch (Exception e) {
					SDFSLogger.getLog().warn("error closing", e);
				}
			}
			try {
				File f = new File(path + ".vrp");
				FileOutputStream fout = new FileOutputStream(f);
				ObjectOutputStream oon = new ObjectOutputStream(fout);
				for (int i = 0; i < numshards; i++) {
					oon.writeObject(removed.get(i));
				}
				oon.flush();
				fout.getFD().sync();
				oon.close();
				fout.flush();

				fout.close();
			} catch (Exception e) {
				SDFSLogger.getLog().warn("error closing", e);
			} /*
				 * if (this.bf != null) { try { FileOutputStream fout = new
				 * FileOutputStream(path + ".bf"); ObjectOutputStream oon = new
				 * ObjectOutputStream(fout); oon.writeObject(this.bf);
				 * oon.flush(); oon.close(); fout.flush(); fout.close(); } catch
				 * (Exception e) { SDFSLogger.getLog().warn("error closing", e);
				 * } }
				 */
			try {
				RandomAccessFile _bpos = new RandomAccessFile(path + ".bpos", "rw");
				_bpos.seek(0);
				_bpos.writeBoolean(full);
				_bpos.writeLong(lastFound.get());
				_bpos.getFD().sync();
				_bpos.close();
			} catch (Exception e) {
				SDFSLogger.getLog().warn("error closing", e);
			}
		} finally {
			l.unlock();
		}
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug("closed " + this.path);
		SDFSLogger.getLog().info("close hashtable " + path + " size = " + this.sz.get() + " map size= " + _sz);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#claimRecords()
	 */
	@Override
	public synchronized long claimRecords() throws IOException {
		this.hashlock.readLock().lock();
		try {
			if (this.closed)
				throw new IOException("Hashtable " + this.path + " is close");
			long k = 0;
			for (Shard sh : this.shards) {
				k += sh.claimRecords();
			}

			return k;
		} finally {
			this.hashlock.readLock().unlock();
		}
	}

	protected void initialize() throws IOException {
		this.sz.set(0);
		/*
		if (this.newInstance) {
			byte[] key = new byte[EL * 43690];
			Arrays.fill(key, (byte) 0);
			SDFSLogger.getLog().info("initialize " + this.path + " key map");
			ByteBuffer bk = ByteBuffer.allocateDirect(key.length);
			bk.put(key);
			kFC.position(0);
			while ((kFC.position() + key.length) < kFC.size() && !this.closed) {
				bk.position(0);

				kFC.write(bk);
			}
			int lft = (int) (kFC.size() - kFC.position());
			key = new byte[lft];
			Arrays.fill(key, (byte) 0);
			bk = ByteBuffer.allocateDirect(key.length);
			bk.put(key);
			bk.position(0);
			kFC.write(bk);
			this.kFC.force(true);
			SDFSLogger.getLog().info("done initialize " + this.path + " key map");
			key = new byte[8 * 43690];
			Arrays.fill(key, (byte) 0);
			SDFSLogger.getLog().info("initialize " + this.path + " ref map");

		}
		
		*/
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#sync()
	 */
	@Override
	public void sync() throws SyncFailedException, IOException {
		// this.kFC.force(true);
	}

	public static class KeyBlob implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = -2753966297671970793L;
		/**
		 * 
		 */
		public byte[] key;

		public KeyBlob(byte[] key) {
			this.key = key;
		}

		public byte[] getKey() {
			return this.key;
		}

		public void setKey(byte[] key) {
			this.key = key;
		}
	}

	Funnel<KeyBlob> kbFunnel = new Funnel<KeyBlob>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = -1612304804452862219L;

		/**
		 * 
		 */

		@Override
		public void funnel(KeyBlob key, PrimitiveSink into) {
			into.putBytes(key.key);
		}
	};

	/*
	 * @Override public long claimRecords(BloomFilter<KeyBlob> bf) throws
	 * IOException { this.iterInit(); byte[] key = this.nextKey(); while (key !=
	 * null) { if (bf.mightContain(new KeyBlob(key))) { this.hashlock.lock();
	 * this.claims.set(this.iterPos - 1); this.hashlock.unlock(); } key =
	 * this.nextKey(); } this.iterInit(); return this.claimRecords(); }
	 */

	@Override
	public long claimRecords(LargeBloomFilter nbf) throws IOException {
		if (this.closed)
			throw new IOException("Hashtable " + this.path + " is close");
		long k = 0;
		for (Shard sh : this.shards) {
			k += sh.claimRecords(nbf);
		}
		if (k > 0) {
			synchronized(this) {
			this.full = false;
			}
			this.sz.addAndGet((int) -1 * (int) k);
		}

		return k;

	}

	@Override
	public long claimRecords(LargeBloomFilter nbf, LargeBloomFilter lbf) throws IOException {
		if (this.closed)
			throw new IOException("Hashtable " + this.path + " is close");
		long k = 0;
		for (Shard sh : this.shards) {
			k += sh.claimRecords(nbf, lbf);
		}
		if (k > 0) {
			synchronized(this) {
			this.full = false;
			}
			this.sz.addAndGet((int) -1 * (int) k);
		}

		return k;
	}

	@Override
	public boolean equals(Object object) {
		boolean sameSame = false;
		if (object != null && object instanceof ShardedFileByteArrayLongMap2) {
			ShardedFileByteArrayLongMap2 m = (ShardedFileByteArrayLongMap2) object;
			sameSame = this.path.equalsIgnoreCase(m.path);
		}
		return sameSame;
	}

	@Override
	public void vanish() {
		Lock l = this.hashlock.writeLock();
		l.lock();
		try {
			this.closed = true;
			for (Shard s : shards) {
				synchronized (s) {
					s.close();
				}
			}

			try {
				this.kFC.close();

			} catch (Exception e) {
				SDFSLogger.getLog().error("error closing", e);
			}

			try {
				this.kRaf.close();
			} catch (Exception e) {
				SDFSLogger.getLog().error("error closing", e);
			}

			boolean del = false;
			int trs = 0;
			File f = new File(path + ".keys".trim());
			while (!del) {

				del = f.delete();
				if (!del)
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
					}
				trs++;
				if (trs > 10) {
					SDFSLogger.getLog()
							.warn("unable to delete " + f.getPath() + " " + f.exists() + " " + this.cacheRunning + " ");
					break;
				}
			}
			f = new File(path + ".bpos");
			f.delete();
			f = new File(path + ".vmp");
			f.delete();
			f = new File(path + ".vrp");
			f.delete();
			f = new File(path + ".bf");
			f.delete();
			// bf = null;
		} finally {
			l.unlock();
		}
	}

	@Override
	public long getLastModified() {
		return new File(path + ".keys").lastModified();
	}

	@Override
	public int compareTo(ShardedFileByteArrayLongMap2 m1) {
		long dif = this.lastFound.get() - m1.lastFound.get();
		if (dif > 0)
			return 1;
		if (dif < 0)
			return -1;
		else
			return 0;
	}

	public static class Shard implements Runnable {
		MappedByteBuffer kFC = null;
		ShardedFileByteArrayLongMap2 m;
		private int size;
		BitSet mapped;
		BitSet claims;
		BitSet removed;
		int currentSz = 0;
		int maxSz = 0;
		int iterPos = 0;
		int start;
		private Object obj = new Object();
		
		public Shard(ShardedFileByteArrayLongMap2 m, int start, int size, BitSet mapped, BitSet claims, BitSet removed)
				throws IOException {
			this.m = m;
			this.size = size;
			this.start = start;
			this.maxSz = (int) (size * m.loadFactor);
			long ep = (long) ((long) size * (long) ShardedFileByteArrayLongMap2.EL);
			long sp = (long) ((long) start * (long) ShardedFileByteArrayLongMap2.EL);
			kFC = m.kFC.map(FileChannel.MapMode.READ_WRITE, sp, ep);
			
			// System.out.println("start=" + start + " ep=" + ep + " fl=" +
			// m.kFC.size());
			this.mapped = mapped;
			this.removed = removed;
			this.claims = claims;
		}

		public synchronized void iterInit() {
			this.iterPos = 0;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub

		}

		private int hashFunc1(int hash) {
			return hash % size;
		}

		private boolean isFree(int pos) {
			if (mapped.get(pos) || removed.get(pos))
				return false;
			else
				return true;
		}

		/**
		 * Locates the index of <tt>obj</tt>.
		 * 
		 * @param obj
		 *            an <code>Object</code> value
		 * @return the index of <tt>obj</tt> or -1 if it isn't in the set.
		 * @throws IOException
		 */
		private int index(byte[] key) throws IOException {

			// From here on we know obj to be non-null
			ByteBuffer buf = ByteBuffer.wrap(key);
			byte[] current = new byte[FREE.length];
			buf.position(8);
			int hash = buf.getInt() & 0x7fffffff;
			int index = this.hashFunc1(hash);
			if (this.isFree(index)) {
				// SDFSLogger.getLog().info("free=" + index + " hash="
				// +StringUtils.getHexString(key));
				return -1;
			} else
				index = index * EL;
			kFC.position(index);
			kFC.get(current);
			if (Arrays.equals(current, key)) {
				// SDFSLogger.getLog().info("found=" + index+ " hash="
				// +StringUtils.getHexString(key));
				return index;
			}
			return indexRehashed(key, index, hash, current);
		}

		/**
		 * Locates the index of non-null <tt>obj</tt>.
		 * 
		 * @param obj
		 *            target key, know to be non-null
		 * @param index
		 *            we start from
		 * @param hash
		 * @param cur
		 * @return
		 * @throws IOException
		 */

		private int indexRehashed(byte[] key, int index, int hash, byte[] cur) throws IOException {

			// NOTE: here it has to be REMOVED or FULL (some user-given value)
			// see Knuth, p. 529
			int length = size * EL;
			int probe = (1 + (hash % (size - 2))) * EL;
			final long loopIndex = index;

			do {
				index -= probe;
				if (index < 0) {
					index += length;
				}
				if (!this.isFree((index / EL))) {
					kFC.position(index);
					kFC.get(cur);
					if (Arrays.equals(cur, key)) {
						return index;
					}
				} else {
					return -1;
				}
			} while (index != loopIndex);
			SDFSLogger.getLog().debug("looped through everything in hashtable");
			return -1;
		}

		private int insertionIndex(byte[] key, boolean migthexist) throws IOException, HashtableFullException {
			ByteBuffer buf = ByteBuffer.wrap(key);
			buf.position(8);
			int hash = buf.getInt() & 0x7fffffff;
			int index = this.hashFunc1(hash);
			byte[] current = new byte[FREE.length];
			if (this.isFree(index)) {
				return index * EL;
			} else
				index = index * EL;
			if (migthexist) {
				kFC.position(index);
				kFC.get(current);
				if (Arrays.equals(current, key)) {
					return -index - 1; // already stored
				}
			}
			return insertKeyRehash(key, index, hash, current, migthexist);
		}

		/**
		 * Looks for a slot using double hashing for a non-null key values and
		 * inserts the value in the slot
		 * 
		 * @param key
		 *            non-null key value
		 * @param index
		 *            natural index
		 * @param hash
		 * @param cur
		 *            value of first matched slot
		 * @return
		 * @throws IOException
		 * @throws HashtableFullException
		 */
		private int insertKeyRehash(byte[] key, int index, int hash, byte[] cur, boolean mightexist)
				throws IOException, HashtableFullException {
			final int length = size * EL;
			final int probe = (1 + (hash % (size - 2))) * EL;
			final int loopIndex = index;
			int firstRemoved = -1;

			/**
			 * Look until FREE slot or we start to loop
			 */
			// int k = 0;

			do {
				// Identify first removed slot

				if (removed.get(index / EL) && firstRemoved == -1) {
					firstRemoved = index;
					if (!mightexist)
						return index;
				}
				index -= probe;
				if (index < 0) {
					index += length;
				}

				// A FREE slot stops the search
				if (this.isFree(index / EL)) {
					if (firstRemoved != -1) {
						return firstRemoved;
					} else {
						return index;
					}
				}
				if (mightexist) {
					kFC.position(index);
					kFC.get(cur);
					if (Arrays.equals(cur, key)) {
						return -index - 1;
					}
				}

				// Detect loop
			} while (index != loopIndex);

			// We inspected all reachable slots and did not find a FREE one
			// If we found a REMOVED slot we return the first one found
			if (firstRemoved != -1) {
				return firstRemoved;
			}

			// Can a resizing strategy be found that resizes the set?
			throw new HashtableFullException("No free or removed slots available. Key set full?!!");
		}

		public boolean containsKey(byte[] key) throws MapClosedException {
			synchronized (obj) {
				try {
					int index = index(key);
					if (index >= 0) {
						int pos = (index / EL);

						synchronized (this.claims) {
							this.claims.set(pos);
						}
						return true;
					}
					return false;
				} catch (Exception e) {
					SDFSLogger.getLog().fatal("error getting record", e);
					return false;
				}
			}
		}

		public boolean claimKey(byte[] key, long val, long ct) {
			synchronized (obj) {
				try {
					int pos = -1;

					pos = this.index(key);
					if (pos == -1) {
						return false;
					} else {
						long _val = this.kFC.getLong(pos + VP);
						if (_val == val) {

							ct += this.kFC.getLong(pos + ZL);
							if (ct < 0) {
								SDFSLogger.getLog().warn("ct< 0 ct=" + ct + " val=" + val);
								ct = 0;
							}
							this.kFC.putLong(pos + ZL, ct);
							if (ct > 0)
								this.claims.set(pos / EL);
							// SDFSLogger.getLog().info("added " + ct + " " +
							// StringUtils.getHexString(key));

							return true;
						} else {
							// SDFSLogger.getLog().info("not " + ct + " " +
							// StringUtils.getHexString(key) + "val=" + val + "
							// _val=" + _val);
							return false;
						}
					}
				} catch (Exception e) {
					SDFSLogger.getLog().fatal("error setting claim", e);
					return false;
				}
			}
		}

		public long get(byte[] key, boolean claim) {
			synchronized (obj) {
				try {
					int pos = -1;

					pos = this.index(key);
					if (pos == -1) {
						return -1;
					} else {
						long val = this.kFC.getLong(pos + VP);

						if (claim) {
							long clr = this.kFC.getLong(pos + ZL);
							if (clr < 0)
								clr = 0;
							this.kFC.position(pos + ZL);
							clr = clr + 1;
							this.kFC.putLong(clr);
							pos = (pos / EL);
							this.claims.set(pos);

						}

						return val;

					}
				} catch (Exception e) {
					SDFSLogger.getLog().fatal("error getting record", e);
					return -1;
				}
			}

		}

		ByteBuffer pbuf = ByteBuffer.allocateDirect(EL);

		public InsertRecord put(byte[] key, long value, long cl, boolean mightContain)
				throws HashtableFullException, IOException {
			synchronized (obj) {
				
				if (!m.active || m.full || this.currentSz >= maxSz) {
					throw new HashtableFullException(
							"entries is greater than or equal to the maximum number of entries. You need to expand"
									+ "the volume or DSE allocation size");

				}
				int pos = -1;
				try {
					pos = this.insertionIndex(key, mightContain);
				} catch (HashtableFullException e) {
					throw e;
				}
				if (cl <= 0)
					cl = 1;
				if (pos < 0) {

					int npos = -pos - 1;
					long nv = this.kFC.getLong(npos + VP);
					long ct = this.kFC.getLong(npos + ZL);
					if (ct < 0)
						ct = 0;
					this.kFC.putLong(npos + ZL, ct + cl);
					// SDFSLogger.getLog().info("added " + ct + " " +
					// StringUtils.getHexString(key));
					this.claims.set(npos / EL);
					this.removed.clear(npos / EL);
					this.mapped.set(npos / EL);
					return new InsertRecord(false, nv);
				} else {
					this.kFC.position(pos);
					this.kFC.put(key);
					this.kFC.putLong(value);
					this.kFC.putLong(cl);
					// this.kFC.put(key);
					// this.kFC.putLong(value);
					pos = (pos / EL);
					// this.rFC.putLong(pos*8, 1);
					this.claims.set(pos);
					this.mapped.set(pos);
					this.currentSz = this.currentSz++;
					this.removed.clear(pos);
					return new InsertRecord(true, value);
				}
			}
		}

		public void close() {
			sun.misc.Cleaner cleaner = ((DirectBuffer) kFC).cleaner();
			cleaner.clean();
		}

		public byte[] nextKey(boolean recreate) throws IOException {
			this.mapped.cardinality();
			while (iterPos < size) {
				synchronized (obj) {
					if (m.isClosed())
						throw new IOException("map closed");
					if (recreate || this.mapped.get(iterPos)) {
						byte[] key = new byte[FREE.length];
						kFC.position(iterPos * EL);
						kFC.get(key);
						iterPos++;
						if (Arrays.equals(key, REMOVED)) {
							this.removed.set(iterPos - 1);
							this.mapped.clear(iterPos - 1);
						} else if (!Arrays.equals(key, FREE)) {
							// System.out.println("m");
							this.mapped.set(iterPos - 1);
							this.removed.clear(iterPos - 1);
							// System.out.println("ip="+iterPos);
							return key;
						} else {
							this.mapped.clear(iterPos - 1);
						}
					} else {

						iterPos++;
					}
				}
			}
			return null;
		}

		public KVPair nextKeyValue() throws IOException {
			while (iterPos < size) {
				synchronized (obj) {

					if (this.mapped.get(iterPos)) {
						byte[] key = new byte[FREE.length];
						int pos = iterPos * EL;
						kFC.position(pos);
						kFC.get(key);
						iterPos++;
						if (Arrays.equals(key, REMOVED)) {
							this.removed.set(iterPos - 1);
							this.mapped.clear(iterPos - 1);
						} else if (!Arrays.equals(key, FREE)) {

							this.mapped.set(iterPos - 1);
							this.removed.clear(iterPos - 1);
							KVPair p = new KVPair();
							p.key = key;
							p.value = kFC.getLong();
							p.loc = kFC.getLong();
							return p;
						} else {
							this.mapped.clear(iterPos - 1);
						}
					} else {
						iterPos++;
					}
				}
			}
			return null;
		}

		public boolean isClaimed(byte[] key) throws KeyNotFoundException, IOException {
			synchronized (obj) {
				int index = index(key);
				if (index >= 0) {
					int pos = (index / EL);
					synchronized (this.claims) {
						boolean zl = this.claims.get(pos);
						if (zl)
							return true;
						else {
							long cl = this.kFC.getLong((pos * EL) + ZL);
							if (cl > 0)
								return true;
						}
					}

				} else {
					throw new KeyNotFoundException(key);
				}
				return false;
			}

		}

		public boolean update(byte[] key, long value) throws IOException {
			synchronized (obj) {
				try {
					int pos = this.index(key);
					if (pos == -1) {
						return false;
					} else {
						this.kFC.position(pos);
						this.kFC.put(key);
						this.kFC.putLong(value);
						this.claims.set(pos);
						this.mapped.set(pos);
						this.removed.clear(pos);
						// this.store.position(pos);
						// this.store.put(storeID);
						return true;
					}
				} catch (Exception e) {
					SDFSLogger.getLog().fatal("error getting record", e);
					return false;
				}
			}
		}

		public boolean remove(byte[] key) throws IOException {
			synchronized (obj) {
				try {

					int pos = this.index(key);

					if (pos == -1) {
						return false;
					}
					boolean claimed = false;
					claimed = this.claims.get(pos / EL);

					if (claimed) {
						return false;
					} else {
						this.kFC.position(pos);
						this.kFC.put(REMOVED);
						long fp = this.kFC.getLong();

						this.kFC.putLong(pos + REMOVED.length, 0);
						this.kFC.putLong(0);
						this.kFC.putLong(0);
						pos = (pos / EL);
						ChunkData ck = new ChunkData(fp, key);
						if (ck.setmDelete(true)) {

							// this.kFC.write(rbuf, pos);
							this.claims.clear(pos);
							this.mapped.clear(pos);
							this.removed.set(pos);
							// this.store.position(pos);
							// this.store.put((byte)0);
							return true;
						} else
							return false;
					}
				} catch (Exception e) {
					SDFSLogger.getLog().fatal("error getting record", e);
					return false;
				}
			}
		}

		public synchronized long claimRecords() throws IOException {

			long k = 0;

			try {
				this.iterInit();
				synchronized (obj) {
					this.claims.clear();
				}
				while (iterPos < size) {

					try {
						synchronized (obj) {

							boolean claimed = claims.get(iterPos);
							claims.clear(iterPos);
							if (claimed) {
								this.mapped.set(iterPos);
								this.removed.clear(iterPos);
							} else if (mapped.get(iterPos)) {
								byte[] key = new byte[FREE.length];
								kFC.position(iterPos * EL);
								kFC.get(key);
								kFC.getLong();
								long val = kFC.getLong();
								if (val <= 0) {
									int pos = iterPos * EL;
									long ov = kFC.getLong(pos + VP);
									kFC.position(pos);

									this.kFC.put(REMOVED);
									this.kFC.putLong(0);
									ChunkData ck = new ChunkData(ov, key);
									ck.setmDelete(true);
									this.mapped.clear(iterPos);
									this.removed.set(iterPos);
									synchronized(m) {
									m.full = false;
									}
									k++;
								}
							}
						}
					} finally {
						iterPos++;
					}
				}
			} catch (NullPointerException e) {

			}
			return k;
		}

		public long claimRecords(LargeBloomFilter nbf) throws IOException {
			long k = 0;

			try {
				this.iterInit();
				synchronized (obj) {
					this.claims.clear();
				}
				while (iterPos < size) {

					try {
						synchronized (obj) {

							boolean claimed = claims.get(iterPos);
							claims.clear(iterPos);
							if (claimed) {
								this.mapped.set(iterPos);
								this.removed.clear(iterPos);
								byte[] key = new byte[FREE.length];
								kFC.position(iterPos * EL);
								kFC.get(key);
								nbf.put(key);

							} else if (mapped.get(iterPos)) {
								byte[] key = new byte[FREE.length];
								kFC.position(iterPos * EL);
								kFC.get(key);
								long ov = kFC.getLong();
								long val = kFC.getLong();
								if (val <= 0) {
									int pos = iterPos * EL;
									kFC.position(pos);
									this.kFC.put(REMOVED);
									this.kFC.putLong(0);
									ChunkData ck = new ChunkData(ov, key);
									ck.setmDelete(true);
									this.mapped.clear(iterPos);
									this.removed.set(iterPos);
									synchronized(m) {
									m.full = false;
									}
									k++;
								} else {
									nbf.put(key);
								}
							}
						}
					} finally {
						iterPos++;
					}
				}
			} catch (NullPointerException e) {

			}
			return k;
		}

		public long claimRecords(LargeBloomFilter nbf, LargeBloomFilter lbf) throws IOException {
			this.iterInit();
			long _sz = 0;

			try {
				// ByteBuffer bk = ByteBuffer.allocateDirect(FREE.length + 8);
				while (iterPos < size) {

					synchronized (obj) {

						try {
							if (m.isClosed())
								throw new IOException("map closed");
							byte[] key = new byte[FREE.length];
							int pos = iterPos * EL;
							kFC.position(pos);
							kFC.get(key);
							long val = kFC.get();
							if (!Arrays.equals(key, FREE) && !Arrays.equals(key, REMOVED)) {
								if (!nbf.mightContain(key) && !this.claims.get(iterPos)) {
									kFC.position(pos);
									this.kFC.put(REMOVED);
									this.kFC.putLong(0);
									this.kFC.putLong(0);
									ChunkData ck = new ChunkData(val, key);
									ck.setmDelete(true);
									this.mapped.clear(iterPos);
									this.removed.set(iterPos);
									_sz++;
								} else {
									lbf.put(key);
									this.mapped.set(iterPos);

								}
								this.claims.clear(iterPos);
							}

						} finally {
							iterPos++;
						}
					}
				}

				return _sz;
			} catch (Exception e) {
				SDFSLogger.getLog().error("error during claim process", e);
				throw new IOException(e);
			} finally {

			}
		}
	}

	public static void main(String[] args) {
		ByteBuffer kbf = ByteBuffer.allocateDirect(16);
		ByteBuffer zbf = ByteBuffer.allocateDirect(16);
		zbf.putLong(16);
		zbf.putLong(16);
		kbf.putLong(16);
		kbf.putLong(16);
		zbf.position(0);
		kbf.position(0);
		System.out.println(kbf.compareTo(zbf));
	}

	@Override
	public void run() {
		/*
		try {
			byte[] key = new byte[EL * 256];
			SDFSLogger.getLog().info("caching " + this.path);
			ByteBuffer bk = ByteBuffer.allocateDirect(key.length);
			bk.put(key);

			this.hashlock.readLock().lock();
			try {
				kFC.position(0);
				while (!this.isClosed() && (kFC.position() + key.length) < kFC.size() && !this.closed) {
					this.hashlock.readLock().unlock();
					bk.position(0);
					this.hashlock.readLock().lock();
					kFC.read(bk);
				}
			} finally {
				try {
					this.hashlock.readLock().unlock();
				} catch (Exception e) {
				}
			}

			int lft = (int) (kFC.size() - kFC.position());
			key = new byte[lft];
			Arrays.fill(key, (byte) 0);
			bk = ByteBuffer.allocateDirect(key.length);
			bk.put(key);
			bk.position(0);
			this.hashlock.readLock().lock();
			try {
				if (!this.isClosed())
					kFC.read(bk);
			} finally {
				this.hashlock.readLock().unlock();
			}
			this.nextCached.set(System.currentTimeMillis() + mixCacheTM);
			SDFSLogger.getLog().info("done caching " + this.path);

		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to cache", e);
		}
		*/

	}

}