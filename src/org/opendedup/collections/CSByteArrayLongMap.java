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
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendedup.collections.threads.SyncThread;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.util.HashFunctions;
import org.opendedup.util.StringUtils;

public class CSByteArrayLongMap implements AbstractMap {
	RandomAccessFile kRaf = null;
	FileChannel kFc = null;
	private long size = 0;
	private ReentrantLock arlock = new ReentrantLock();
	private ReentrantLock iolock = new ReentrantLock();
	private static Logger log = Logger.getLogger("sdfs");
	private byte[] FREE = new byte[24];
	private byte[] REMOVED = new byte[24];
	private byte[] BLANKCM = new byte[ChunkData.RAWDL];
	private long resValue = -1;
	private long freeValue = -1;
	private String fileName;
	private List<ChunkData> kBuf = Collections
			.synchronizedList(new ArrayList<ChunkData>());
	private ByteArrayLongMap[] maps = new ByteArrayLongMap[16];
	private boolean removingChunks = false;
	private String fileParams = "rw";

	private boolean closed = true;
	int kSz = 0;
	long ram = 0;
	private static final int kBufMaxSize = 1000;
	ByteBuffer freeSlots = ByteBuffer.allocateDirect(8 * 1000000);

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

	public CSByteArrayLongMap(long maxSize, short arraySize, String fileName,
			String fileParams) throws IOException {
		this.fileParams = fileParams;
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
			iolock.lock();
			arlock.lock();
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
				arlock.unlock();
				iolock.unlock();
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
		if (this.isClosed())
			throw new IOException("Hashtable " + this.fileName + " is close");
		RandomAccessFile _fs = new RandomAccessFile(this.fileName, this.fileParams);
		long startTime = System.currentTimeMillis();
		int z = 0;
		for (int i = 0; i < maps.length; i++) {
			try {
				maps[i].iterInit();
				long val = 0;

				while (val != -1 && !this.closed) {
					try {
						this.iolock.lock();
						val = maps[i].nextClaimedValue(true);
						if (val != -1) {
							long pos = ((val / (long) Main.chunkStorePageSize) * (long) ChunkData.RAWDL)
									+ ChunkData.CLAIMED_OFFSET;
							z++;
							_fs.seek(pos);
							_fs.writeLong(System.currentTimeMillis());
						}
					} catch (Exception e) {
					} finally {
						this.iolock.unlock();
					}

				}
			} catch (NullPointerException e) {

			}
		}
		try {
			_fs.close();
			_fs = null;
		} catch (Exception e) {

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
		kRaf = new RandomAccessFile(fileName, this.fileParams);
		// kRaf.setLength(ChunkMetaData.RAWDL * size);
		kFc = kRaf.getChannel();

		this.freeSlots.clear();
		log.finer("Populating free slots");
		while (this.freeSlots.position() < this.freeSlots.capacity()) {
			this.freeSlots.putLong(-1);
		}
		log.finer("Populated free slots");
		long start = System.currentTimeMillis();
		int freeSl = 0;
		if (exists) {
			this.closed = false;
			log.info("This looks an existing hashtable will repopulate with ["
					+ size + "] entries.");
			log
					.info("##################### Loading Hash Database #####################");
			kRaf.seek(0);
			this.freeSlots.position(0);
			int count = 0;
			
			while (kFc.position() < kRaf.length()) {
				count++;
				if (count > 500000) {
					count = 0;
					System.out.print("#");
				}
				

				byte[] raw = new byte[ChunkData.RAWDL];
				try {
					long currentPos = kFc.position();
					kRaf.read(raw);
					if (Arrays.equals(raw, BLANKCM)
							&& (this.freeSlots.position() < this.freeSlots
									.capacity())) {
						log
								.finer("found free slot at "
										+ ((currentPos / raw.length) * Main.chunkStorePageSize));
						this.freeSlots.putLong((currentPos / raw.length)
								* Main.chunkStorePageSize);
						freeSl++;
					} else {
						ChunkData cm = new ChunkData(raw);
						boolean corrupt = false;
						/*
						 * try { byte[] chkHash =
						 * HashFunctions.getTigerHashBytes(cm .getData()); if
						 * (!Arrays.equals(chkHash, cm.getHash())) { corrupt =
						 * true; log.severe("corrupt data found at " +
						 * cm.getcPos()); cm.setmDelete(true);
						 * this.flushFullBuffer(); this.kBuf.add(cm);
						 * this.freeSlots .putLong((currentPos / raw.length)
						 * Main.chunkStorePageSize); } } catch (Exception e) {
						 * log.severe("Error while checking and hashing data");
						 * throw new IOException(e); }
						 */
						if (!corrupt) {
							boolean foundFree = Arrays.equals(cm.getHash(),
									FREE);
							boolean foundReserved = Arrays.equals(cm.getHash(),
									REMOVED);
							long value = cm.getcPos();
							if (!cm.ismDelete()) {
								if (foundFree) {
									this.freeValue = value;
									log.info("found free  key  with value "
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
					}
				} catch (BufferUnderflowException e) {

				}
			}
			this.freeSlots.position(0);
		}
		System.out.println(" Done Loading ");
		log
				.info("########## Finished Loading Hash Database in ["+(System.currentTimeMillis() - start)/100 +"] seconds ###########");
		log.info("loaded [" + kSz + "] into the hashtable [" + this.fileName
				+ "] free slots available are [" + freeSl + "]");
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

	public void removeRecords(long time) throws IOException {
		if (this.isClosed())
			throw new IOException("Hashtable " + this.fileName + " is close");
		this.freeSlotLock.lock();
		try {
			this.removingChunks = true;
		} catch (Exception e) {
		} finally {
			this.freeSlotLock.unlock();
		}
		ByteBuffer _oldSlots = freeSlots;
		freeSlots = ByteBuffer.allocateDirect(8 * 1000000);
		_oldSlots.position(0);
		try {
			while (_oldSlots.position() < _oldSlots.capacity()) {
				long pos = _oldSlots.getLong();
				if (pos != -1) {
					freeSlots.putLong(pos);
				}
			}
		} catch (Exception e) {
			log.log(Level.WARNING,
					"unable to populate freeSlots with old slots", e);
		}

		try {
			RandomAccessFile _fs = new RandomAccessFile(this.fileName, fileParams);
			long rem = 0;
			for (int i = 0; i < size; i++) {
				byte[] raw = new byte[ChunkData.RAWDL];
				try {
					this.iolock.lock();
					try {
						_fs.read(raw);
					} catch (Exception e) {
					} finally {
						this.iolock.unlock();
					}
					if (!Arrays.equals(raw, BLANKCM)) {
						try {
							ChunkData cm = new ChunkData(raw);
							boolean foundFree = Arrays.equals(cm.getHash(),
									FREE);
							boolean foundReserved = Arrays.equals(cm.getHash(),
									REMOVED);
							if (cm.getLastClaimed() < time && !foundFree
									&& !foundReserved) {
								cm.setmDelete(true);
								if (this.freeSlots.position() < this.freeSlots
										.capacity()) {
									this.freeSlots.putLong(cm.getcPos());
								}
								if (this.remove(cm)) {
									rem++;
								}
							}
						} catch (Exception e1) {
							log.log(Level.WARNING,
									"unable to access record at "
											+ _fs.getChannel().position(), e1);
						}
					}
				} catch (BufferUnderflowException e) {

				}

			}

			_fs.close();
			_fs = null;
			log.info("Removed [" + rem + "] records. Free slots ["
					+ (this.freeSlots.position() / 8) + "]");
		} catch (Exception e) {
		} finally {
			this.freeSlots.position(0);
			this.flushBuffer(true);
			this.removingChunks = false;
		}
	}

	public boolean put(ChunkData cm) throws IOException {
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
				this.arlock.lock();
				this.kBuf.add(cm);
			} catch (Exception e) {
				throw new IOException(e.toString());
			} finally {
				this.arlock.unlock();
			}
			added = true;
		} else if (foundReserved) {
			this.resValue = cm.getcPos();
			try {
				this.arlock.lock();
				this.kBuf.add(cm);
			} catch (Exception e) {
				throw new IOException(e.toString());
			} finally {
				this.arlock.unlock();
			}
			added = true;
		} else {
			added = this.put(cm, true);
		}
		if (added)
			kSz++;
		return added;
	}

	private ReentrantLock freeSlotLock = new ReentrantLock();

	private long getFreeSlot() {
		if (this.removingChunks)
			return -1;
		freeSlotLock.lock();
		try {
			int sPos = this.freeSlots.position();
			if (this.freeSlots.position() < this.freeSlots.capacity()) {
				long pos = this.freeSlots.getLong();
				if (pos != -1) {
					this.freeSlots.position(sPos);
					this.freeSlots.putLong(-1);
				}
				return pos;
			} else {
				this.freeSlots.position(0);
			}
		} catch (Exception e) {

		} finally {
			this.freeSlotLock.unlock();
		}
		return -1;
	}

	private void addFreeSolt(long position) {
		if (this.removingChunks)
			return;
		freeSlotLock.lock();
		try {
			int sPos = this.freeSlots.position();
			while (this.freeSlots.position() < this.freeSlots.capacity()) {
				long pos = this.freeSlots.getLong();
				if (pos == -1) {
					this.freeSlots.position(freeSlots.position() - 8);
					this.freeSlots.putLong(position);
					this.freeSlots.position(sPos);
					return;
				}
			}
		} catch (Exception e) {

		} finally {
			this.freeSlotLock.unlock();
		}

	}

	private boolean put(ChunkData cm, boolean persist) throws IOException {
		if (this.isClosed())
			throw new IOException("Hashtable " + this.fileName + " is close");
		if (kSz >= this.size)
			throw new IOException("maximum sized reached");
		boolean added = false;
		if (persist)
			this.flushFullBuffer();
		if (persist) {
			cm.setcPos(this.getFreeSlot());
			cm.persistData(true);
			added = this.getMap(cm.getHash()).put(cm.getHash(), cm.getcPos());
			if (added) {
				this.arlock.lock();
				try {
					this.kBuf.add(cm);
				} catch (Exception e) {
				} finally {
					this.arlock.unlock();
				}
			} else {
				this.addFreeSolt(cm.getcPos());
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

	public byte[] getData(byte[] key) throws IOException {
		if (this.isClosed())
			throw new IOException("Hashtable " + this.fileName + " is close");
		long ps = this.get(key);
		if (ps != -1) {
			return ChunkData.getChunk(key, ps);
		} else {
			log.info("found no data for key [" + StringUtils.getHexString(key)
					+ "]");
			return null;
		}

	}

	private synchronized void flushBuffer(boolean lock) throws IOException {
		List<ChunkData> oldkBuf = null;
		try {
			if (lock)
				this.arlock.lock();
			oldkBuf = kBuf;
			if (this.isClosed())
				kBuf = null;
			else {
				kBuf = Collections.synchronizedList(new ArrayList<ChunkData>());
			}
		} catch (Exception e) {
		} finally {
			if (lock)
				this.arlock.unlock();
		}
		Iterator<ChunkData> iter = oldkBuf.iterator();
		while (iter.hasNext()) {
			ChunkData cm = iter.next();
			if (cm != null) {
				long pos = (cm.getcPos() / (long) Main.chunkStorePageSize)
						* (long) ChunkData.RAWDL;
				this.iolock.lock();
				try {
					kFc.position(pos);
					kFc.write(cm.getMetaDataBytes());
				} catch (Exception e) {
				} finally {
					this.iolock.unlock();
				}
				cm = null;
			}
		}
		oldkBuf.clear();
		oldkBuf = null;

	}

	public boolean remove(ChunkData cm) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		this.flushFullBuffer();
		try {
			if (!this.getMap(cm.getHash()).remove(cm.getHash())) {
				return false;
			} else {
				cm.setmDelete(true);
				this.arlock.lock();
				try {
					this.kBuf.add(cm);
				} catch (Exception e) {
				} finally {
					this.arlock.unlock();
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
			// this.hashlock.lock();
			if (kBuf.size() >= kBufMaxSize) {
				flush = true;
			}
		} catch (Exception e) {
		} finally {
			// this.hashlock.unlock();
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
			this.flushBuffer(true);
			this.kRaf.getFD().sync();
			this.kRaf.close();
			this.kRaf = null;
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Hashtable [" + this.fileName + "] closed");
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
