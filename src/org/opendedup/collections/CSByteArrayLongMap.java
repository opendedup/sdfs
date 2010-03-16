package org.opendedup.collections;

import java.io.File;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendedup.collections.threads.SyncThread;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkMetaData;
import org.opendedup.util.HashFunctions;
import org.opendedup.util.StringUtils;

public class CSByteArrayLongMap implements AbstractMap {
	RandomAccessFile kRaf = null;
	FileChannel kFc = null;
	private long size = 0;
	private ReentrantLock hashlock = new ReentrantLock();
	private static Logger log = Logger.getLogger("sdfs");
	private byte[] FREE = new byte[24];
	private byte[] REMOVED = new byte[24];
	private byte[] BLANKCM = new byte[ChunkMetaData.RAWDL];
	private long resValue = -1;
	private long freeValue = -1;
	private String fileName;
	private ArrayList<ChunkMetaData> kBuf = new ArrayList<ChunkMetaData>();
	private ByteArrayLongMap[] maps = new ByteArrayLongMap[16];
	private boolean closed = true;
	int kSz = 0;
	long ram = 0;
	private static final int kBufMaxSize = 1000;

	public CSByteArrayLongMap(long maxSize, short arraySize, String fileName)
			throws IOException {
		this.size = maxSize;
		this.fileName = fileName;
		FREE = new byte[arraySize];
		REMOVED = new byte[arraySize];
		Arrays.fill(FREE, (byte) 0);
		Arrays.fill(BLANKCM, (byte) 0);
		Arrays.fill(REMOVED, (byte) 1);
		this.setUp();
		this.closed = false;
		new SyncThread(this);
	}

	public ByteArrayLongMap getMap(byte[] hash) throws IOException {
		byte hashRoute = (byte) (hash[1] / (byte) 8);
		if (hashRoute < 0) {
			hashRoute += 1;
			hashRoute *= -1;
		}
		ByteArrayLongMap m = maps[hashRoute];
		if (m == null) {
			hashlock.lock();
			try {
				m = maps[hashRoute];
				if (m == null) {
					int sz = (int) (size / 16);
					ram = ram + (sz * (24 + 8));

					m = new ByteArrayLongMap(sz, (short) FREE.length);
					maps[hashRoute] = m;
				}
			} catch (Exception e) {
				log.log(Level.SEVERE, "unable to create hashmap. "
						+ maps.length, e);
				throw new IOException(e);
			} finally {
				hashlock.unlock();
			}
		}

		return m;
	}

	public long getAllocatedRam() {
		return this.ram;
	}

	public boolean isClosed() {
		return this.closed;
	}

	public int getSize() {
		return this.kSz;

	}

	public long getMaxSize() {
		return this.size;
	}

	public synchronized void claimRecords() throws IOException {
		RandomAccessFile _fs = new RandomAccessFile(this.fileName, "rw");
		long startTime = System.currentTimeMillis();
		int z = 0;
		for (int i = 0; i < maps.length; i++) {
			maps[i].iterInit();
			long val = 0;

			while (val != -1 && !this.closed) {
				val = maps[i].nextClaimedValue(true);
				if (val != -1) {
					long pos = ((val / (long) Main.chunkStorePageSize) * (long) ChunkMetaData.RAWDL)
							+ ChunkMetaData.CLAIMED_OFFSET;
					z++;
					_fs.seek(pos);
					this.hashlock.lock();
					try {
						_fs.writeLong(val);
					} catch (Exception e) {
					} finally {
						this.hashlock.unlock();
					}
				}
			}
		}
		log.info("processed [" + (z) + "] records in ["
				+ (System.currentTimeMillis() - startTime) + "] ms");
	}

	/**
	 * initializes the Object set of this hash table.
	 * 
	 * @param initialCapacity
	 *            an <code>int</code> value
	 * @return an <code>int</code> value
	 * @throws FileNotFoundException
	 */
	public long setUp() throws IOException {
		boolean exists = new File(fileName).exists();
		long fileSize = this.size * (FREE.length + 8);
		if (fileSize > Integer.MAX_VALUE)
			throw new IOException("File size is limited to "
					+ Integer.MAX_VALUE + " proposed fileSize would be "
					+ fileSize + " try "
					+ "reducing the number of entries or the key size");
		kRaf = new RandomAccessFile(fileName, "rw");
		// kRaf.setLength(ChunkMetaData.RAWDL * size);
		kFc = kRaf.getChannel();
		if (exists) {
			log.info("This looks an existing hashtable will repopulate with ["
					+ size + "] entries.");
			kRaf.seek(0);
			for (int i = 0; i < size; i++) {
				byte[] raw = new byte[ChunkMetaData.RAWDL];
				try {
					kRaf.read(raw);
					if (!Arrays.equals(raw, BLANKCM)) {
						ChunkMetaData cm = new ChunkMetaData(raw);
						boolean foundFree = Arrays.equals(cm.getHash(), FREE);
						boolean foundReserved = Arrays.equals(cm.getHash(),
								REMOVED);
						long value = cm.getcPos();
						if (!cm.ismDelete()) {
							if (foundFree) {
								this.freeValue = value;
								log
										.info("found free  key  with value "
												+ value);
							} else if (foundReserved) {
								this.resValue = value;
								log.info("found reserve  key  with value "
										+ value);
							} else {
								this.put(cm, false);
								kSz++;
							}
						}
					}
				} catch (BufferUnderflowException e) {

				}
			}
		}
		log.info("loaded [" + kSz + "] into the hashtable " + this.fileName);
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
	@SuppressWarnings( { "unchecked" })
	public boolean containsKey(byte[] key) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		return this.getMap(key).containsKey(key);
	}

	public void removeRecords(long time) throws IOException {
		for (int i = 0; i < size; i++) {
			byte[] raw = new byte[ChunkMetaData.RAWDL];
			try {
				kRaf.read(raw);
				if (!Arrays.equals(raw, BLANKCM)) {
					ChunkMetaData cm = new ChunkMetaData(raw);
					boolean foundFree = Arrays.equals(cm.getHash(), FREE);
					boolean foundReserved = Arrays
							.equals(cm.getHash(), REMOVED);
					if (cm.getLastClaimed() < time && !foundFree
							&& !foundReserved) {
						cm.setmDelete(true);
						if (this.remove(cm)) {
							kSz++;
						}
					}
				}
			} catch (BufferUnderflowException e) {

			}

		}
		log.info("Removed [" + kSz + "] records");
	}

	public boolean put(ChunkMetaData cm) throws IOException {
		if (cm.getHash().length != this.FREE.length)
			throw new IOException("key length mismatch");
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		this.flushFullBuffer();
		boolean added = false;
		boolean foundFree = Arrays.equals(cm.getHash(), FREE);
		boolean foundReserved = Arrays.equals(cm.getHash(), REMOVED);
		if (foundFree) {
			this.freeValue = cm.getcPos();
			try {
				this.hashlock.lock();
				this.kBuf.add(cm);
			} catch (Exception e) {
				throw new IOException(e.toString());
			} finally {
				this.hashlock.unlock();
			}
			added = true;
		} else if (foundReserved) {
			this.resValue = cm.getcPos();
			try {
				this.hashlock.lock();
				this.kBuf.add(cm);
			} catch (Exception e) {
				throw new IOException(e.toString());
			} finally {
				this.hashlock.unlock();
			}
			added = true;
		} else {
			added = this.put(cm, true);
		}
		if (added)
			kSz++;
		return added;
	}

	private boolean put(ChunkMetaData cm, boolean persist) throws IOException {
		if (kSz >= this.size)
			throw new IOException("maximum sized reached");
		if (persist)
			this.flushFullBuffer();

		boolean added = this.getMap(cm.getHash()).put(cm.getHash(),
				cm.getcPos());
		if (added && persist) {
			this.hashlock.lock();
			try {
				this.kBuf.add(cm);
			} catch (Exception e) {
			} finally {
				this.hashlock.unlock();
			}
		}
		return added;
	}

	public boolean update(ChunkMetaData cm) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		return false;
		/*
		 * try { this.hashlock.lock(); int pos = this.index(cm.getHash()); if
		 * (pos == -1) { return false; } else { this.keys.position(pos);
		 * this.keys.put(cm.getHash()); pos = (pos / FREE.length) * 8;
		 * this.values.position(pos); this.values.putLong(cm.getcPos()); pos =
		 * (pos/8)*4; cm.updateNumClaimed(this.claims.getInt());
		 * this.kBuf.add(cm); return true; }
		 * 
		 * } catch (Exception e) { log.log(Level.SEVERE, "error record record",
		 * e); return false; } finally { this.hashlock.unlock(); }
		 */
	}

	public long get(byte[] key) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		return this.getMap(key).get(key);

	}

	private synchronized void flushBuffer(boolean lock) throws IOException {
		ArrayList<ChunkMetaData> oldkBuf = null;
		try {
			if (lock)
				this.hashlock.lock();
			oldkBuf = kBuf;
			kBuf = new ArrayList<ChunkMetaData>();
		} catch (Exception e) {
		} finally {
			if (lock)
				this.hashlock.unlock();
		}
		Iterator<ChunkMetaData> iter = oldkBuf.iterator();
		while (iter.hasNext()) {
			ChunkMetaData cm = iter.next();
			if (cm != null) {
				long pos = (cm.getcPos() / (long) Main.chunkStorePageSize)
						* (long) ChunkMetaData.RAWDL;
				kFc.position(pos);
				kFc.write(cm.getBytes());
			}
			// log.info("write "+b+" at " + pos + " pos " + cm.getcPos() +
			// " len " + Main.chunkStorePageSize + " mdlen " +
			// ChunkMetaData.RAWDL);
		}
		oldkBuf.clear();
		oldkBuf = null;

	}

	public boolean remove(ChunkMetaData cm) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		this.flushFullBuffer();
		try {
			if (!this.getMap(cm.getHash()).remove(cm.getHash())) {
				return false;
			} else {
				cm.setmDelete(true);
				this.hashlock.lock();
				try {
					this.kBuf.add(cm);
				} catch (Exception e) {
				} finally {
					this.hashlock.unlock();
				}
				return true;
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "error getting record", e);
			return false;
		}
	}

	private synchronized void flushFullBuffer() throws IOException {
		boolean flush = false;
		try {
			this.hashlock.lock();
			if (kBuf.size() >= kBufMaxSize) {
				flush = true;
			}
		} catch (Exception e) {
		} finally {
			this.hashlock.unlock();
		}
		if (flush) {
			this.flushBuffer(true);
		}
	}

	public synchronized void sync() throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		this.flushBuffer(true);
		this.kRaf.getFD().sync();
	}

	public void close() {
		this.closed = true;
		try {
			this.sync();
		} catch (Exception e) {
		}
	}

	@Override
	public byte[] get(long pos) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void put(long pos, byte[] data) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void remove(long pos) throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public void vanish() throws IOException {
		// TODO Auto-generated method stub

	}
}
