package org.opendedup.collections;

import gnu.trove.iterator.TLongIterator;

import gnu.trove.set.hash.TLongHashSet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.collections.threads.SyncThread;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.util.NextPrime;
import org.opendedup.util.SDFSLogger;
import org.opendedup.util.StringUtils;

public class CSByteArrayLongMap implements AbstractMap {
	RandomAccessFile kRaf = null;
	FileChannel kFc = null;
	private long size = 0;
	private ReentrantLock arlock = new ReentrantLock();
	private ReentrantLock iolock = new ReentrantLock();
	private byte[] FREE = new byte[Main.hashLength];
	private byte[] REMOVED = new byte[Main.hashLength];
	private byte[] BLANKCM = new byte[ChunkData.RAWDL];
	private long resValue = -1;
	private long freeValue = -1;
	private String fileName;
	private List<ChunkData> kBuf = new ArrayList<ChunkData>();
	private ByteArrayLongMap[] maps = null;
	private boolean removingChunks = false;
	private String fileParams = "rw";
	private static int freeSlotsLength = 3000000;
	// The amount of memory available for free slots.
	private boolean closed = true;
	long kSz = 0;
	long ram = 0;
	private long maxSz = 0;
	private int hashRoutes = 0;
	// TODO change the kBufMazSize so it not reflective to the pageSize
	private static final int kBufMaxSize = 10485760 / Main.chunkStorePageSize;
	TLongHashSet freeSlots = new TLongHashSet(freeSlotsLength);
	TLongIterator iter = null;
	private boolean firstGCRun = true;

	public CSByteArrayLongMap(long maxSize, String fileName)
			throws IOException, HashtableFullException {
		if (Main.compressedIndex)
			maps = new ByteArrayLongMap[65535];
		else
			maps = new ByteArrayLongMap[16];
		this.size = (long) (maxSize);
		this.maxSz = maxSize;
		this.fileName = fileName;
		FREE = new byte[Main.hashLength];
		REMOVED = new byte[Main.hashLength];
		Arrays.fill(FREE, (byte) 0);
		Arrays.fill(BLANKCM, (byte) 0);
		Arrays.fill(REMOVED, (byte) 1);
		this.setUp();
		this.closed = false;
		new SyncThread(this);
	}

	public CSByteArrayLongMap(long maxSize, String fileName, String fileParams)
			throws IOException, HashtableFullException {
		if (Main.compressedIndex)
			maps = new ByteArrayLongMap[65535];
		else
			maps = new ByteArrayLongMap[16];
		this.fileParams = fileParams;
		this.size = (long) (maxSize * 1.125);
		this.maxSz = maxSize;
		this.fileName = fileName;
		FREE = new byte[Main.hashLength];
		REMOVED = new byte[Main.hashLength];
		Arrays.fill(FREE, (byte) 0);
		Arrays.fill(BLANKCM, (byte) 0);
		Arrays.fill(REMOVED, (byte) 1);
		this.setUp();
		this.closed = false;
		new SyncThread(this);
	}

	public ByteArrayLongMap getMap(byte[] hash) throws IOException {
		int hashb = 0;
		if (Main.compressedIndex) {
			hashb = ByteBuffer.wrap(hash).getShort(hash.length - 2);
			if (hashb < 0) {
				hashb = ((hashb * -1) + 32766);
			}
		} else {
			hashb = hash[2];
			if (hashb < 0) {
				hashb = ((hashb * -1) + 127);
			}
		}
		int hashRoute = hashb / 16;

		ByteArrayLongMap m = maps[hashRoute];
		if (m == null) {
			iolock.lock();
			arlock.lock();
			try {
				m = maps[hashRoute];
				if (m == null) {

					// int propsize = (int) (size / maps.length);
					int sz = NextPrime
							.getNextPrimeI((int) (size / maps.length));
					// SDFSLogger.getLog().debug("will create byte array of size "
					// + sz + " propsize was " + propsize);
					ram = ram + (sz * (Main.hashLength + 8));
					m = new ByteArrayLongMap(sz, (short) (FREE.length));
					maps[hashRoute] = m;
					// System.out.println("Creating map at " + hashb +
					// " for total of " + this.mapsCreated);
				}
				hashRoutes++;
				// SDFSLogger.getLog().debug("hashroute [" + hashRoute +
				// "] created hr=" + this.hashRoutes);
			} catch (Exception e) {
				SDFSLogger.getLog().fatal(
						"unable to create hashmap. " + maps.length, e);
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

	public long getSize() {
		return this.kSz;

	}

	public long getMaxSize() {
		return this.size;
	}

	public synchronized void claimRecords() throws IOException {
		if (this.isClosed())
			throw new IOException("Hashtable " + this.fileName + " is close");
		SDFSLogger.getLog().info("claiming records");
		RandomAccessFile _fs = new RandomAccessFile(this.fileName,
				this.fileParams);
		long startTime = System.currentTimeMillis();
		int z = 0;
		int k = 0;
		for (int i = 0; i < maps.length; i++) {
			try {
				maps[i].iterInit();
				long val = 0;
				while (val != -1 && !this.closed) {
					k++;
					if (k > 310) {
						k = 0;
						try {
						} catch (Exception e) {
						}
					}
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
		SDFSLogger.getLog().info(
				"processed [" + (z) + "] records in ["
						+ (System.currentTimeMillis() - startTime) + "] ms");
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
	public long setUp() throws IOException, HashtableFullException {
		File _fs = new File(fileName);
		boolean exists = new File(fileName).exists();
		if (!_fs.getParentFile().exists()) {
			_fs.getParentFile().mkdirs();
		}
		kRaf = new RandomAccessFile(fileName, this.fileParams);
		// kRaf.setLength(ChunkMetaData.RAWDL * size);
		kFc = kRaf.getChannel();
		this.freeSlots.clear();
		long start = System.currentTimeMillis();
		int freeSl = 0;
		if (exists) {
			this.closed = false;
			SDFSLogger.getLog().info(
					"This looks an existing hashtable will repopulate with ["
							+ size + "] entries.");
			SDFSLogger
					.getLog()
					.info("##################### Loading Hash Database #####################");
			kRaf.seek(0);
			int count = 0;
			System.out.print("Loading ");
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
					if (Arrays.equals(raw, BLANKCM)) {
						SDFSLogger
								.getLog()
								.debug("found free slot at "
										+ ((currentPos / raw.length) * Main.chunkStorePageSize));
						this.addFreeSlot((currentPos / raw.length)
								* Main.chunkStorePageSize);
						freeSl++;
					} else {
						ChunkData cm = null;
						boolean corrupt = false;
						try {
							cm = new ChunkData(raw);
						}catch(Exception e) {
							SDFSLogger.getLog().info("HashTable corrupt!");
							corrupt = true;
						}
						
						
						if (!corrupt) {
							boolean foundFree = Arrays.equals(cm.getHash(),
									FREE);
							boolean foundReserved = Arrays.equals(cm.getHash(),
									REMOVED);
							long value = cm.getcPos();
							if (!cm.ismDelete()) {
								if (foundFree) {
									this.freeValue = value;
									SDFSLogger.getLog().info(
											"found free  key  with value "
													+ value);
								} else if (foundReserved) {
									this.resValue = value;
									SDFSLogger.getLog().info(
											"found reserve  key  with value "
													+ value);
								} else {
									if (cm.getHash().length > 0) {
										boolean added = this.put(cm, false);
										if (added)
											this.kSz++;
									} else {
										SDFSLogger
												.getLog()
												.debug("found free slot at "
														+ ((currentPos / raw.length) * Main.chunkStorePageSize));
										this.addFreeSlot((currentPos / raw.length)
												* Main.chunkStorePageSize);
										freeSl++;
									}
								}
							}
						}
					}
				} catch (BufferUnderflowException e) {

				}
			}
		}
		System.out.println(" Done Loading ");
		SDFSLogger.getLog().info(
				"########## Finished Loading Hash Database in ["
						+ (System.currentTimeMillis() - start) / 100
						+ "] seconds ###########");
		SDFSLogger.getLog().info(
				"loaded [" + kSz + "] into the hashtable [" + this.fileName
						+ "] free slots available are [" + freeSl
						+ "] free slots added [" + this.freeSlots.size() + "]");
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

	public synchronized void removeRecords(long time) throws IOException {
		SDFSLogger.getLog().info(
				"Garbage Collection records older than " + new Date(time));
		if (this.firstGCRun) {
			this.firstGCRun = false;
			throw new IOException(
					"Garbage Collection aborted because it is the first run");
		} else {

			SDFSLogger.getLog().info("removing free blocks");
			if (this.isClosed())
				throw new IOException("Hashtable " + this.fileName
						+ " is close");
			RandomAccessFile _fs = null;
			try {
				_fs = new RandomAccessFile(this.fileName, "r");
				long rem = 0;
				for (int i = 0; i < size; i++) {
					byte[] raw = new byte[ChunkData.RAWDL];
					try {
						_fs.read(raw);
						if (!Arrays.equals(raw, BLANKCM)) {
							try {
								ChunkData cm = new ChunkData(raw);
								if (cm.getLastClaimed() < time) {
									if (this.remove(cm)) {
										this.addFreeSlot(cm.getcPos());
										rem++;
									}
								} else {
									cm = null;
								}
							} catch (Exception e1) {
								SDFSLogger.getLog().warn(
										"unable to access record at "
												+ _fs.getChannel().position(),
										e1);
							} finally {
								raw = null;
							}
						}
					} catch (BufferUnderflowException e) {

					} finally {

					}

				}

				SDFSLogger.getLog().info(
						"Removed [" + rem + "] records. Free slots ["
								+ this.freeSlots.size() + "]");
			} catch (Exception e) {
			} finally {
				try {
					_fs.close();
				} catch (Exception e) {
				}
				_fs = null;
				this.flushBuffer(true);
				this.removingChunks = false;

			}
			System.gc();
		}
	}

	public boolean put(ChunkData cm) throws IOException, HashtableFullException {
		if (this.kSz >= this.maxSz)
			throw new IOException(
					"entries is greater than or equal to the maximum number of entries. You need to expand"
							+ "the volume or DSE allocation size");
		if (cm.getHash().length != this.FREE.length)
			throw new IOException("key length mismatch");
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		// this.flushFullBuffer();
		boolean added = false;
		boolean foundFree = Arrays.equals(cm.getHash(), FREE);
		boolean foundReserved = Arrays.equals(cm.getHash(), REMOVED);
		if (foundFree) {
			this.freeValue = cm.getcPos();
			try {
				this.arlock.lock();
				this.kBuf.add(cm);
			} catch (Exception e) {
				throw new IOException(e);
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
				throw new IOException(e);
			} finally {
				this.arlock.unlock();
			}
			added = true;
		} else {
			added = this.put(cm, true);
		}

		return added;
	}

	private ReentrantLock freeSlotLock = new ReentrantLock();

	private long getFreeSlot() {
		if (this.removingChunks)
			return -1;
		freeSlotLock.lock();

		try {
			if (this.freeSlots.size() == 0) {
				return -1;
			}
			if (this.iter == null)
				this.iter = this.freeSlots.iterator();
			try {
				long nxt = this.iter.next();
				this.iter.remove();
				if (nxt >= 0)
					return nxt;
				else
					return -1;
			} catch (ConcurrentModificationException e) {
				SDFSLogger.getLog().debug("hash table modified");
				this.iter = this.freeSlots.iterator();
				long nxt = this.iter.next();
				this.iter.remove();
				if (nxt >= 0)
					return nxt;
				else
					return -1;
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.freeSlotLock.unlock();
		}
		return -1;
	}

	private void addFreeSlot(long position) {
		if (this.removingChunks)
			return;
		freeSlotLock.lock();
		try {
			if (this.freeSlots.size() < freeSlotsLength) {
				try {
					if (!this.freeSlots.contains(position)) {
						this.freeSlots.add(position);
					}

				} catch (Exception e) {
					e.printStackTrace();
				} finally {

				}
			}
		} catch (Exception e) {

		} finally {
			this.freeSlotLock.unlock();
		}
	}

	private boolean put(ChunkData cm, boolean persist) throws IOException,
			HashtableFullException {
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
			added = this.getMap(cm.getHash()).put(cm.getHash(), cm.getcPos(),
					(byte) 1);
			if (added) {
				this.arlock.lock();
				try {
					this.kBuf.add(cm);
				} catch (Exception e) {
				} finally {
					this.kSz++;
					this.arlock.unlock();

				}
			} else {
				this.addFreeSlot(cm.getcPos());
				cm = null;
			}
		} else {
			added = this.getMap(cm.getHash()).put(cm.getHash(), cm.getcPos(),
					(byte) 1);
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
		boolean foundFree = Arrays.equals(key, FREE);
		boolean foundReserved = Arrays.equals(key, REMOVED);
		if (foundFree) {
			return this.freeValue;
		}
		if (foundReserved) {
			return this.resValue;
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

	private void flushBuffer(boolean lock) throws IOException {
		List<ChunkData> oldkBuf = null;
		
		try {
			if (lock) {
				this.arlock.lock();
				if (kBuf.size() == 0) {
					return;
				}
			}
			oldkBuf = kBuf;
			if (this.isClosed())
				kBuf = null;
			else {
				kBuf = new ArrayList<ChunkData>();
			}
		} finally {
			if (lock)
				this.arlock.unlock();
		}
		if (oldkBuf.size() > 0) {
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
						cm = null;
						this.iolock.unlock();
					}
					cm = null;
				}
			}
			oldkBuf.clear();

		}
		oldkBuf = null;

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
				try {
					this.kBuf.add(cm);
				} catch (Exception e) {
				} finally {
					kSz--;
					this.arlock.unlock();
				}

				return true;
			}
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error getting record", e);
			return false;
		}
	}

	protected void flushFullBuffers() throws IOException {
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

	private ReentrantLock syncLock = new ReentrantLock();

	public void sync() throws IOException {
		syncLock.lock();
		try {
			if (this.isClosed()) {
				throw new IOException("hashtable [" + this.fileName
						+ "] is close");
			}
			this.flushBuffer(true);
			// this.kRaf.getFD().sync();
		} finally {
			syncLock.unlock();
		}
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
	public void vanish() throws IOException {
		// TODO Auto-generated method stub

	}
}
