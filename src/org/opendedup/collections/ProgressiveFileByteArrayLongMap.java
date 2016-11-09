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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.opendedup.collections.AbstractShard;
import org.opendedup.collections.HashtableFullException;
import org.opendedup.collections.KeyNotFoundException;
import org.opendedup.collections.ProgressiveFileBasedCSMap.ProcessPriorityThreadFactory;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.hashing.LargeBloomFilter;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;

import org.opendedup.utils.hashing.FileBasedBloomFilter;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public class ProgressiveFileByteArrayLongMap
		implements AbstractShard, Serializable, Runnable, Comparable<ProgressiveFileByteArrayLongMap> {
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
	transient protected static final int EL = HashFunctionPool.hashLength + 8;
	transient private static final int VP = HashFunctionPool.hashLength;
	transient private ReentrantReadWriteLock hashlock = new ReentrantReadWriteLock();
	transient public static byte[] FREE = new byte[HashFunctionPool.hashLength];
	transient public static byte[] REMOVED = new byte[HashFunctionPool.hashLength];
	transient private int iterPos = 0;
	transient private boolean closed = false;
	transient private BitSet claims = null;
	transient private BitSet removed = null;
	transient private BitSet mapped = null;
	transient private AtomicInteger sz = new AtomicInteger(0);
	transient private FileBasedBloomFilter<KeyBlob> bf = null;
	transient private boolean runningGC;
	transient private long bgst = 0;
	transient boolean full = false;
	transient private boolean compacting = false;
	transient private boolean active = false;
	transient private boolean cached = false;
	public transient long lastFound = 0;
	private static transient BlockingQueue<Runnable> loadCacheQueue = new SynchronousQueue<Runnable>();
	private static transient ThreadPoolExecutor loadCacheExecutor = new ThreadPoolExecutor(1, 10, 10, TimeUnit.SECONDS,
			loadCacheQueue, new ProcessPriorityThreadFactory(Thread.MIN_PRIORITY));

	static {
		FREE = new byte[HashFunctionPool.hashLength];
		REMOVED = new byte[HashFunctionPool.hashLength];
		Arrays.fill(FREE, (byte) 0);
		Arrays.fill(REMOVED, (byte) 1);
	}

	public ProgressiveFileByteArrayLongMap(String path, int size) throws IOException {
		this.size = size;
		this.path = path;
	}

	public void compactRunning(boolean running) {
		this.compacting = running;
	}

	public boolean equals(ProgressiveFileByteArrayLongMap m) {
		if (m == null)
			return false;
		if (m.path == null)
			return false;
		if (m.path.equals(this.path))
			return true;
		else
			return false;

	}

	public boolean isCompactig() {
		return this.compacting;
	}

	public void inActive() {
		Lock l = this.hashlock.writeLock();
		l.lock();
		this.active = false;
		l.unlock();
	}

	public void activate() {
		Lock l = this.hashlock.writeLock();
		l.lock();
		this.lastFound = System.currentTimeMillis();
		this.active = true;
		l.unlock();
	}

	public boolean isActive() {
		Lock l = this.hashlock.readLock();
		l.lock();
		try {
			return this.active;
		} finally {
			l.unlock();
		}
	}

	public synchronized void cache() {
		long lr = System.currentTimeMillis() - lastRead.get();
		if (lr > mtm) {
			if (!this.cacheRunning) {
				this.cacheRunning = true;
				lr = System.currentTimeMillis() - lastRead.get();
				if (lr > mtm) {
					try {
						if (this.isClosed())
							throw new IOException("map closed");
						if (!this.cached) {
							loadCacheExecutor.execute(this);
							this.cached = true;
						}
					} catch (Exception e) {
						if (SDFSLogger.isDebug())
							SDFSLogger.getLog().debug("unable to cache " + this, e);

					}
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#iterInit()
	 */
	@Override
	public synchronized void iterInit() {
		this.iterPos = 0;
	}

	public boolean isFull() {
		if (full)
			return true;
		else
			full = this.sz.get() >= maxSz;
		return full;
	}

	public boolean isMaxed() {
		double nms = (double) maxSz + ((double) maxSz * .1);
		return this.sz.get() >= nms;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#nextKey()
	 */
	@Override
	public byte[] nextKey() throws IOException {
		while (iterPos < size) {
			Lock l = this.hashlock.writeLock();
			l.lock();
			try {
				if (this.isClosed())
					throw new IOException("map closed");
				if (this.mapped.get(iterPos)) {
					byte[] key = new byte[FREE.length];
					kFC.read(ByteBuffer.wrap(key), (long) iterPos * (long) EL);
					iterPos++;
					if (Arrays.equals(key, REMOVED)) {
						this.removed.set(iterPos - 1);
						this.mapped.clear(iterPos - 1);
					} else if (!Arrays.equals(key, FREE)) {
						this.mapped.set(iterPos - 1);
						this.removed.clear(iterPos - 1);

						this.bf.put(key);
						return key;
					} else {
						this.mapped.clear(iterPos - 1);
					}
				} else {
					iterPos++;
				}
			} finally {
				l.unlock();
			}

		}
		return null;
	}

	public KVPair nextKeyValue() throws IOException {
		while (iterPos < size) {
			Lock l = this.hashlock.writeLock();
			l.lock();
			try {
				if (this.isClosed())
					throw new IOException("map closed");
				if (this.mapped.get(iterPos)) {
					byte[] key = new byte[FREE.length];
					long pos = (long) iterPos * (long) EL;
					kFC.read(ByteBuffer.wrap(key), pos);
					// kFC.writeToArray(pos, key, 0, key.length);
					iterPos++;
					if (Arrays.equals(key, REMOVED)) {
						this.removed.set(iterPos - 1);
						this.mapped.clear(iterPos - 1);
					} else if (!Arrays.equals(key, FREE)) {
						this.mapped.set(iterPos - 1);
						this.removed.clear(iterPos - 1);
						this.bf.put(key);
						KVPair p = new KVPair();
						p.key = key;
						ByteBuffer bk = ByteBuffer.allocate(8);

						kFC.read(bk, pos + key.length);
						bk.flip();
						p.value = bk.getLong();
						return p;
					} else {
						this.mapped.clear(iterPos - 1);
					}
				} else {
					iterPos++;
				}
			} finally {
				l.unlock();
			}

		}
		return null;
	}

	private byte[] _nextKey() throws IOException {
		while (iterPos < size) {
			Lock l = this.hashlock.writeLock();
			l.lock();
			try {
				byte[] key = new byte[FREE.length];
				kFC.read(ByteBuffer.wrap(key),(long) iterPos * (long) EL);
				//kFC.writeToArray((long) iterPos * (long) EL, key, 0, key.length);
				iterPos++;
				if (Arrays.equals(key, REMOVED)) {
					this.removed.set(iterPos - 1);
					this.mapped.clear(iterPos - 1);
				} else if (!Arrays.equals(key, FREE)) {
					this.mapped.set(iterPos - 1);
					this.removed.clear(iterPos - 1);
					this.bf.put(key);
					return key;
				} else {
					this.mapped.clear(iterPos - 1);
				}
			} finally {
				l.unlock();
			}

		}
		return null;
	}

	private void recreateMap() throws IOException {
		Lock l = this.hashlock.writeLock();
		l.lock();
		try {
			mapped = new BitSet(size);
			mapped.clear();
			removed = new BitSet(size);
			removed.clear();
			bf = FileBasedBloomFilter.create(kbFunnel, size, .01, new File(path + ".nbf").getPath(), !Main.LOWMEM);
			this.iterInit();
			byte[] key = this._nextKey();
			while (key != null)
				key = this._nextKey();
			SDFSLogger.getLog().warn("Recovered Hashmap " + this.path + " entries = " + mapped.cardinality());
		} finally {
			l.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#getBigestKey()
	 */
	@Override
	public long getBigestKey() throws IOException {
		this.iterInit();
		long _bgst = 0;
		Lock l = this.hashlock.readLock();
		l.lock();
		try {
			ByteBuffer bk = ByteBuffer.allocateDirect(8);
			while (iterPos < size) {
				bk.position(0);
				kFC.read(bk,((long) iterPos * (long) EL) + VP);
				bk.position(0);
				long val = bk.getLong();

				iterPos++;
				if (val > _bgst)
					_bgst = val;
			}
		} finally {
			l.unlock();
		}
		return _bgst;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#setUp()
	 */

	boolean setup = false;

	@Override
	public synchronized long setUp() throws IOException {
		if (!setup) {
			File posFile = new File(path + ".keys");
			boolean newInstance = !posFile.exists();
			if (posFile.exists()) {
				long _sz = (posFile.length()) / (long) (EL);
				if (_sz != size) {
					SDFSLogger.getLog().warn("Resetting size of hashtable to [" + _sz + "] instead of [" + size + "]");
					this.size = (int) _sz;
				}
			}
			this.maxSz = (int) (size * loadFactor);
			// SDFSLogger.getLog().info("sz=" + size + " maxSz=" + this.maxSz);
			long nsz = 0;
			if (newInstance) {
				nsz = (long) this.size * (long) EL;
			} else {
				nsz = posFile.length();
			}
			// SDFSLogger.getLog().info("set table to size " + nsz);
			@SuppressWarnings("resource")
			RandomAccessFile kRaf = new RandomAccessFile(path + ".keys", "rw");
			if (newInstance) {
				kRaf.setLength(nsz);
			}
			SDFSLogger.getLog().info("set table to size " + posFile.length());
			this.kFC = kRaf.getChannel();
			try {
				/*
				 * Field fd = tRaf.getClass().getDeclaredField("fd");
				 * fd.setAccessible(true);
				 * NativePosixUtil.advise((FileDescriptor) fd.get(tRaf), 0, 0,
				 * NativePosixUtil.DONTNEED);
				 * NativePosixUtil.advise((FileDescriptor) fd.get(tRaf), 0, 0,
				 * NativePosixUtil.RANDOM); fd =
				 * kFC.getClass().getDeclaredField("fd");
				 * fd.setAccessible(true);
				 * NativePosixUtil.advise((FileDescriptor) fd.get(kFC), 0, 0,
				 * NativePosixUtil.DONTNEED);
				 * NativePosixUtil.advise((FileDescriptor) fd.get(kFC), 0, 0,
				 * NativePosixUtil.RANDOM);
				 */
			} catch (Exception e) {
				SDFSLogger.getLog().fatal("unable to set advisory", e);
				throw new IOException(e);
			}

			boolean closedCorrectly = true;
			if (newInstance) {
				mapped = new BitSet(size);
				removed = new BitSet(size);
				bf = FileBasedBloomFilter.create(kbFunnel, size, .01, new File(path + ".nbf").getPath(), !Main.LOWMEM);
				this.full = false;
			} else {
				File f = new File(path + ".bpos");
				if (!f.exists()) {
					closedCorrectly = false;
					SDFSLogger.getLog().warn("bpos does not exist");
				} else {
					try {
						RandomAccessFile _bpos = new RandomAccessFile(path + ".bpos", "rw");
						_bpos.seek(0);
						bgst = _bpos.readLong();
						this.full = _bpos.readBoolean();
						try {
							this.lastFound = _bpos.readLong();
						} catch (Exception e) {

						}
						this.lastFound = System.currentTimeMillis();
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

						mapped = (BitSet) oon.readObject();
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

						removed = (BitSet) oon.readObject();
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
					SDFSLogger.getLog().warn("bf does not exist for " + this.path);
				} else {
					try {
						bf = FileBasedBloomFilter.create(kbFunnel, size, .01, new File(path + ".nbf").getPath(),
								!Main.LOWMEM);
					} catch (Exception e) {
						SDFSLogger.getLog().warn("bf load error", e);
						closedCorrectly = false;
					}
				}
			}
			if (this.lastFound == 0)
				this.lastFound = new File(path + ".keys").lastModified();
			if (!closedCorrectly) {
				this.recreateMap();
			}
			if (bgst < 0) {
				SDFSLogger.getLog().info("Hashtable " + path + " did not close correctly. scanning ");
				bgst = this.getBigestKey();

			}
			sz.set(mapped.cardinality());

			claims = new BitSet(size);
			claims.clear();
			// double pfull = (double) this.sz.get() / (double) size;
			// SDFSLogger.getLog().info("Percentage full=" + pfull + " full=" +
			// this.full);
			this.setup = true;
			return bgst;
		} else {
			throw new IOException("already setup");
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#containsKey(byte[])
	 */
	@Override
	public boolean containsKey(byte[] key) throws MapClosedException {
		Lock l = this.hashlock.readLock();
		l.lock();
		try {
			if (this.isClosed())
				throw new MapClosedException();
			if (!this.runningGC && !bf.mightContain(key)) {
				return false;
			}
			long index = index(key);
			if (index >= 0) {
				int pos = (int) (index / EL);

				synchronized (this.claims) {
					this.claims.set(pos);
				}

				if (this.runningGC) {
					l.unlock();
					l = this.hashlock.writeLock();
					l.lock();
					this.bf.put(key);
				}
				this.lastFound = System.currentTimeMillis();
				return true;
			}
			return false;
		} catch (MapClosedException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error getting record", e);
			return false;
		} finally {
			l.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#isClaimed(byte[])
	 */
	@Override
	public boolean isClaimed(byte[] key) throws KeyNotFoundException, IOException {
		Lock l = this.hashlock.readLock();
		l.lock();
		try {
			if (this.isClosed())
				throw new IOException("map closed");
			long index = index(key);
			if (index >= 0) {
				int pos = (int) (index / EL);
				synchronized (this.claims) {
					boolean zl = this.claims.get(pos);
					if (zl)
						return true;
				}

			} else {
				throw new KeyNotFoundException(key);
			}
			return false;
		} finally {
			l.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#update(byte[], long)
	 */
	@Override
	public boolean update(byte[] key, long value) throws IOException {
		Lock l = this.hashlock.writeLock();
		l.lock();
		try {
			if (this.isClosed())
				throw new IOException("map closed");
			long pos = this.index(key);
			if (pos == -1) {
				return false;
			} else {
				// keys.position(pos);
				if (value > bgst)
					bgst = value;
				this.lastFound = System.currentTimeMillis();
				ByteBuffer bk = ByteBuffer.allocate(key.length + 8);
				bk.put(key);
				bk.putLong(value);
				bk.position(0);
				this.kFC.write(bk,pos);
				pos = (pos / EL);
				synchronized (this.claims) {
					this.claims.set((int) pos);
				}
				if (this.runningGC) {
					this.bf.put(key);
				}
				this.mapped.set((int) pos);
				this.removed.clear((int) pos);

				// this.store.position(pos);
				// this.store.put(storeID);
				return true;
			}
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error getting record", e);
			return false;
		} finally {
			l.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#remove(byte[])
	 */

	@Override
	public boolean remove(byte[] key) throws IOException {
		Lock l = this.hashlock.writeLock();
		l.lock();
		try {
			if (this.isClosed())
				throw new IOException("map closed");
			if (!this.runningGC && !bf.mightContain(key))
				return false;

			long pos = this.index(key);

			if (pos == -1) {
				return false;
			}
			boolean claimed = false;
			synchronized (this.claims) {
				claimed = this.claims.get((int) (pos / EL));
			}

			if (claimed) {
				if (this.runningGC)
					this.bf.put(key);
				return false;
			} else {
				this.kFC.write(ByteBuffer.wrap(REMOVED),pos);
				ByteBuffer bk = ByteBuffer.allocate(8);
				this.kFC.read(bk,pos + REMOVED.length);
				bk.position(0);
				long fp = bk.getLong();
				bk.position(0);
				bk.putLong(0);
				bk.position(0);
				this.kFC.read(bk,pos + REMOVED.length);
				

				ChunkData ck = new ChunkData(fp, key);
				if (ck.setmDelete(true)) {

					// this.kFC.write(rbuf, pos);
					pos = (pos / EL);
					synchronized (this.claims) {
						this.claims.clear((int) pos);
					}

					this.mapped.clear((int) pos);
					this.sz.decrementAndGet();
					this.removed.set((int) pos);
					// this.store.position(pos);
					// this.store.put((byte)0);
					this.full = false;
					return true;
				} else
					return false;
			}
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error getting record", e);
			return false;
		} finally {
			l.unlock();
		}
	}

	private int hashFunc1(int hash) {
		return hash % size;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#hashFunc3(int)
	 */
	public int hashFunc3(int hash) {
		int result = hash + 1;
		return result;
	}

	private boolean isFree(int pos) {
		if (this.mapped.get(pos) || this.removed.get(pos))
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
	private long index(byte[] key) throws IOException {

		// From here on we know obj to be non-null
		ByteBuffer buf = ByteBuffer.wrap(key);
		byte[] current = new byte[FREE.length];
		buf.position(8);
		int hash = buf.getInt() & 0x7fffffff;
		long index = this.hashFunc1(hash);

		if (this.isFree((int) index)) {
			// SDFSLogger.getLog().info("free=" + index + " hash="
			// +StringUtils.getHexString(key));
			return -1;
		} else
			index = index * EL;
		kFC.read(ByteBuffer.wrap(current),index);
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

	private long indexRehashed(byte[] key, long index, int hash, byte[] cur) throws IOException {

		// NOTE: here it has to be REMOVED or FULL (some user-given value)
		// see Knuth, p. 529
		long length = (long) size * (long) EL;
		long probe = (long) (1 + (hash % (size - 2))) * (long) EL;
		final long loopIndex = index;

		do {
			index -= probe;
			if (index < 0) {
				index += length;
			}
			if (!this.isFree((int) (index / EL))) {
				this.kFC.read(ByteBuffer.wrap(cur),index);
				if (Arrays.equals(cur, key)) {
					return index;
				}
			} else {
				return -1;
			}
		} while (index != loopIndex);
		SDFSLogger.getLog().info("looped through everything");
		return -1;
	}

	private long insertionIndex(byte[] key, boolean migthexist) throws IOException, HashtableFullException {
		ByteBuffer buf = ByteBuffer.wrap(key);
		buf.position(8);
		int hash = buf.getInt() & 0x7fffffff;
		long index = this.hashFunc1(hash);
		byte[] current = new byte[FREE.length];
		if (this.isFree((int) index))
			return index * EL;
		else
			index = index * EL;
		if (migthexist) {
			kFC.read(ByteBuffer.wrap(current),index);
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
	private long insertKeyRehash(byte[] key, long index, int hash, byte[] cur, boolean mightexist)
			throws IOException, HashtableFullException {
		final long length = (long) size * (long) EL;
		final long probe = (long) (1 + (hash % (size - 2))) * (long) EL;
		final long loopIndex = index;
		long firstRemoved = -1;

		/**
		 * Look until FREE slot or we start to loop
		 */
		do {
			// Identify first removed slot

			if (this.removed.get((int) (index / EL)) && firstRemoved == -1) {
				firstRemoved = index;
				if (!mightexist)
					return index;
			}
			index -= probe;
			if (index < 0) {
				index += length;
			}

			// A FREE slot stops the search
			if (this.isFree((int) (index / EL))) {
				if (firstRemoved != -1) {
					return firstRemoved;
				} else {
					return index;
				}
			}
			if (mightexist) {
				kFC.read(ByteBuffer.wrap(cur),index);
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

	// transient ByteBuffer zlb = ByteBuffer.wrap(new byte[EL]);

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#put(byte[], long)
	 */
	@Override
	public InsertRecord put(ChunkData cm) throws HashtableFullException, IOException, MapClosedException {
		Lock l = this.hashlock.writeLock();
		l.lock();

		try {
			if (this.isClosed())
				throw new MapClosedException();
			byte[] key = cm.getHash();

			if (!this.active || this.full || this.sz.get() >= maxSz) {
				this.full = true;
				this.active = false;
				throw new HashtableFullException(
						"entries is greater than or equal to the maximum number of entries. You need to expand"
								+ "the volume or DSE allocation size");
			}
			long pos = -1;
			try {
				if (this.runningGC)
					pos = this.insertionIndex(key, true);
				else
					pos = this.insertionIndex(key, bf.mightContain(key));
			} catch (HashtableFullException e) {
				this.full = true;
				throw e;
			}
			if (pos < 0) {

				long npos = (-pos - 1) * -1;
				npos = (npos / EL);
				synchronized (this.claims) {
					this.claims.set((int) npos);
				}
				this.bf.put(key);
				return new InsertRecord(false, this.get(key));
			} else {
				if (!cm.recoverd) {
					try {
						cm.persistData(true);
					} catch (HashExistsException e) {
						return new InsertRecord(false, e.getPos());
					}
				}
				ByteBuffer bk = ByteBuffer.allocate(key.length + 8);
				bk.put(key);
				bk.putLong(cm.getcPos());
				bk.position(0);
				this.kFC.write(bk,pos);
				
				if (cm.getcPos() > bgst)
					bgst = cm.getcPos();
				pos = (pos / EL);
				synchronized (this.claims) {
					this.claims.set((int) pos);
				}
				this.mapped.set((int) pos);
				this.sz.incrementAndGet();
				this.removed.clear((int) pos);
				this.bf.put(key);
				return new InsertRecord(true, cm.getcPos());
			}
			// this.store.position(pos);
			// this.store.put(storeID);

		} finally {
			l.unlock();
		}
	}

	public InsertRecord put(byte[] key, long value) throws HashtableFullException, IOException, MapClosedException {
		Lock l = this.hashlock.writeLock();
		l.lock();
		try {
			if (this.isClosed())
				throw new IOException("map closed");
			if (!this.active || this.full || this.sz.get() >= maxSz) {
				this.full = true;
				this.active = false;
				throw new HashtableFullException(
						"entries is greater than or equal to the maximum number of entries. You need to expand"
								+ "the volume or DSE allocation size");

			}
			long pos = -1;
			try {
				if (this.runningGC)
					pos = this.insertionIndex(key, true);
				else
					pos = this.insertionIndex(key, bf.mightContain(key));
				// SDFSLogger.getLog().info("pos=" + pos/EL + " hash="
				// +StringUtils.getHexString(key));
			} catch (HashtableFullException e) {
				this.full = true;
				throw e;
			}
			if (pos < 0) {
				long npos = -pos - 1;
				npos = (npos / EL);
				synchronized (this.claims) {
					this.claims.set((int) npos);
				}
				this.bf.put(key);
				return new InsertRecord(false, this.get(key));
			} else {
				ByteBuffer bk = ByteBuffer.allocate(key.length + 8);
				bk.put(key);
				bk.putLong(value);
				bk.position(0);
				this.kFC.write(bk,pos);
				if (value > bgst)
					bgst = value;
				pos = (pos / EL);
				synchronized (this.claims) {
					this.claims.set((int) pos);
				}
				this.mapped.set((int) pos);
				this.sz.incrementAndGet();
				this.removed.clear((int) pos);
				this.bf.put(key);
				// this.store.position(pos);
				// this.store.put(storeID);

				return new InsertRecord(true, value);
			}
		} finally {
			l.unlock();
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
		Lock l = this.hashlock.readLock();
		l.lock();
		try {
			if (this.isClosed())
				throw new MapClosedException();
			if (key == null)
				return -1;
			if (!this.runningGC && !this.bf.mightContain(key)) {
				return -1;
			}
			long pos = -1;

			pos = this.index(key);
			if (pos == -1) {
				return -1;
			} else {
				this.lastFound = System.currentTimeMillis();
				long val = -1;
				ByteBuffer bk = ByteBuffer.allocate(8);
				this.kFC.read(bk,pos + VP);
				bk.position(0);
				val = bk.getLong();
				if (claim) {
					pos = (pos / EL);
					synchronized (this.claims) {
						this.claims.set((int) pos);
					}
				}
				if (this.runningGC) {
					l.unlock();
					l = this.hashlock.writeLock();
					l.lock();
					this.bf.put(key);
				}
				return val;

			}
		} catch (MapClosedException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error getting record", e);
			return -1;
		} finally {
			l.unlock();
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

	public int avail() {
		return size - this.sz.get();
	}

	public int maxSize() {
		return this.size;
	}

	public String toString() {
		return this.path;
	}

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
		try {
			this.closed = true;
			try {
				this.kFC.force(true);

			} catch (Exception e) {

			}
			try {
				this.kFC.close();

			} catch (Exception e) {

			}

			

			try {
				File f = new File(path + ".vmp");
				FileOutputStream fout = new FileOutputStream(f);
				ObjectOutputStream oon = new ObjectOutputStream(fout);
				oon.writeObject(mapped);
				oon.flush();
				fout.getFD().sync();
				oon.close();
				fout.flush();

				fout.close();
			} catch (Exception e) {
				SDFSLogger.getLog().warn("error closing", e);
			}
			try {
				File f = new File(path + ".vrp");
				FileOutputStream fout = new FileOutputStream(f);
				ObjectOutputStream oon = new ObjectOutputStream(fout);
				oon.writeObject(this.removed);
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
				RandomAccessFile _bpos = new RandomAccessFile(path + ".bpos", "rw");
				_bpos.seek(0);
				_bpos.writeLong(bgst);
				_bpos.writeBoolean(full);
				_bpos.writeLong(lastFound);
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
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#claimRecords()
	 */
	@Override
	public synchronized long claimRecords() throws IOException {
		if (this.closed)
			throw new IOException("Hashtable " + this.path + " is close");
		long k = 0;

		try {
			this.iterInit();
			while (iterPos < size) {
				Lock l = this.hashlock.writeLock();
				l.lock();
				try {
					synchronized (this.claims) {

						boolean claimed = claims.get(iterPos);
						claims.clear(iterPos);
						if (claimed) {
							this.mapped.set(iterPos);
							this.removed.clear(iterPos);

							k++;
						}
					}
				} finally {
					iterPos++;
					l.unlock();
				}
			}
		} catch (NullPointerException e) {

		}

		return k;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractShard#sync()
	 */
	@Override
	public void sync() throws SyncFailedException, IOException {
		this.kFC.force(true);
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
		this.iterInit();
		long _sz = 0;
		Lock l = this.hashlock.writeLock();
		l.lock();
		try {
			int asz = size;
			if (!this.active)
				asz = this.mapped.cardinality();
			bf = null;
			bf = FileBasedBloomFilter.create(kbFunnel, asz, .01, new File(path + ".nbf").getPath(), !Main.LOWMEM);
			this.runningGC = true;
		} finally {
			l.unlock();
		}
		try {
			ByteBuffer bk = ByteBuffer.allocateDirect(FREE.length + 8);
			while (iterPos < size) {
				l = this.hashlock.writeLock();
				l.lock();
				synchronized (this.claims) {
					bk.position(0);
					try {
						if (this.isClosed())
							throw new IOException("map closed");
						byte[] key = new byte[FREE.length];
						long pos = (long) iterPos * (long) EL;
						kFC.read(bk,pos);
						bk.position(0);
						bk.get(key);
						long val = bk.getLong();
						
						if (!Arrays.equals(key, FREE) && !Arrays.equals(key, REMOVED)) {
							if (!nbf.mightContain(key) && !this.claims.get(iterPos)) {
								bk.position(0);
								bk.put(REMOVED);
								bk.putLong(0);
								bk.position(0);
								this.kFC.write(bk,pos);
								ChunkData ck = new ChunkData(val, key);
								ck.setmDelete(true);
								this.mapped.clear(iterPos);
								this.sz.decrementAndGet();
								this.removed.set(iterPos);
								this.full = false;
								_sz++;
							} else {
								this.mapped.set(iterPos);
								bf.put(key);
							}
							this.claims.clear(iterPos);
						}

					} finally {
						iterPos++;
						l.unlock();

					}
				}
			}
			l = this.hashlock.writeLock();
			l.lock();
			this.runningGC = false;
			l.unlock();
			return _sz;
		} finally {

		}
	}

	public long claimRecords(LargeBloomFilter nbf, LargeBloomFilter lbf) throws IOException {
		this.iterInit();
		long _sz = 0;
		Lock l = this.hashlock.writeLock();
		l.lock();
		try {
			bf = null;
			if (this.active)
				bf = FileBasedBloomFilter.create(kbFunnel, size, .01, new File(path + ".nbf").getPath(), !Main.LOWMEM);
			else
				bf = FileBasedBloomFilter.create(kbFunnel, sz.get(), .01, new File(path + ".nbf").getPath(),
						!Main.LOWMEM);
			this.runningGC = true;
		} catch (Exception e) {
			SDFSLogger.getLog().error("error during claim process", e);
			throw new IOException(e);
		} finally {
			l.unlock();
		}
		try {
			ByteBuffer bk = ByteBuffer.allocateDirect(FREE.length + 8);
			while (iterPos < size) {
				l = this.hashlock.writeLock();
				l.lock();
				synchronized (this.claims) {
					bk.position(0);
					try {
						if (this.isClosed())
							throw new IOException("map closed");
						byte[] key = new byte[FREE.length];
						long pos = (long) iterPos * (long) EL;
						kFC.read(bk,pos);
						bk.position(0);
						bk.get(key);
						long val = bk.getLong();
						if (!Arrays.equals(key, FREE) && !Arrays.equals(key, REMOVED)) {
							if (!nbf.mightContain(key) && !this.claims.get(iterPos)) {
								bk.position(0);
								bk.put(REMOVED);
								bk.putLong(0);
								bk.position(0);
								this.kFC.write(bk,pos);
								
								ChunkData ck = new ChunkData(val, key);
								ck.setmDelete(true);
								this.mapped.clear(iterPos);
								this.sz.decrementAndGet();
								this.removed.set(iterPos);
								_sz++;
								this.full = false;
							} else {
								this.mapped.set(iterPos);
								bf.put(key);
								lbf.put(key);
							}
							this.claims.clear(iterPos);
						}

					} finally {
						iterPos++;
						l.unlock();
					}
				}
			}
			l.lock();
			this.runningGC = false;
			l.unlock();
			return _sz;
		} catch (Exception e) {
			SDFSLogger.getLog().error("error during claim process", e);
			throw new IOException(e);
		} finally {
			l = this.hashlock.writeLock();

		}
	}

	@Override
	public boolean equals(Object object) {
		boolean sameSame = false;
		if (object != null && object instanceof ProgressiveFileByteArrayLongMap) {
			ProgressiveFileByteArrayLongMap m = (ProgressiveFileByteArrayLongMap) object;
			sameSame = this.path.equalsIgnoreCase(m.path);
		}
		return sameSame;
	}

	public void vanish() {
		Lock l = this.hashlock.writeLock();
		l.lock();
		try {
			this.closed = true;
			
			try {
				int trs = 0;
				while(this.kFC.isOpen()) {
					this.kFC.close();
					trs++;
					if (trs > 10) {
						SDFSLogger.getLog().warn("unable to close " + this.toString()+ " " + this.cacheRunning + " " + this.kFC.isOpen());
						break;
					}
				}

			} catch (Exception e) {
				SDFSLogger.getLog().error("error closing",e);
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
					SDFSLogger.getLog().warn("unable to delete " + f.getPath() + " " + f.exists() + " " + this.cacheRunning + " " + this.kFC.isOpen());
					break;
				}
			}
			f = new File(path + ".bpos");
			f.delete();
			f = new File(path + ".vmp");
			f.delete();
			f = new File(path + ".vrp");
			f.delete();
			f = new File(path + ".nbf");
			f.delete();
			if (bf != null)
				this.bf.vanish();

			bf = null;
		} finally {
			l.unlock();
		}
	}

	protected void initialize() throws IOException {
		int ip = 0;
		byte[] key = new byte[EL * 43690];
		Arrays.fill(key, (byte) 0);
		SDFSLogger.getLog().info("initialize " + this.path);
		ByteBuffer bk = ByteBuffer.allocateDirect(key.length);
		while ((ip + (53690)) < size && !this.closed) {
			bk.position(0);
			long pos = (long) ip * (long) EL;
			kFC.write(bk,pos);
			ip += 43690;
		}
		SDFSLogger.getLog().info("done initialize " + this.path);

	}

	AtomicLong lastRead = new AtomicLong(0);
	private static long mtm = 60 * 1000 * 45;// 45 minutes
	private boolean cacheRunning = false;

	@Override
	public void run() {
		try {
		this.cacheRunning = true;

		synchronized (lastRead) {
			try {
				long lr = System.currentTimeMillis() - lastRead.get();
				if (lr > mtm) {
					File posFile = new File(path + ".keys");
					long cp = 0;
					ByteBuffer bk = ByteBuffer.allocateDirect(1024 * 1024 * 5);
					while ((cp + bk.capacity()) < posFile.length() && !this.closed) {
						
						kFC.read(bk);
						cp += bk.capacity();
					}
					byte [] key = new byte[(int) (posFile.length() - cp)];
					if (key.length > 0)
						kFC.read(ByteBuffer.wrap(key));
					SDFSLogger.getLog().info("done reading " + this.path);
					lastRead.set(System.currentTimeMillis());
				}

			} finally {
				this.cacheRunning = false;
			}
		}
		}catch(Exception e) {
			SDFSLogger.getLog().warn("error caching", e);
		}
	}

	public long getLastModified() {
		return new File(path + ".keys").lastModified();
	}

	@Override
	public int compareTo(ProgressiveFileByteArrayLongMap m1) {
		long dif = this.lastFound - m1.lastFound;
		if (dif > 0)
			return 1;
		if (dif < 0)
			return -1;
		else
			return 0;
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

}
