package org.opendedup.collections;

import java.io.File;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.collections.threads.SyncThread;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.servers.HashChunkService;
import org.opendedup.util.NextPrime;
import org.opendedup.util.SDFSLogger;
import org.opendedup.util.StringUtils;

import sun.nio.ch.FileChannelImpl;

public class CSByteArrayLongMap implements AbstractMap, AbstractHashesMap {
	RandomAccessFile kRaf = null;
	FileChannelImpl kFc = null;
	private long size = 0;
	private final ReentrantLock arlock = new ReentrantLock();
	private final ReentrantLock iolock = new ReentrantLock();
	private byte[] FREE = new byte[Main.hashLength];
	private byte[] REMOVED = new byte[Main.hashLength];
	private byte[] BLANKCM = new byte[ChunkData.RAWDL];
	private long resValue = -1;
	private long freeValue = -1;
	private String fileName;
	private List<ChunkData> kBuf = new ArrayList<ChunkData>(30000);
	private ByteArrayLongMap[] maps = null;
	// private boolean removingChunks = false;
	private String fileParams = "rw";
	// private static int freeSlotsLength = 3000000;
	// The amount of memory available for free slots.
	private boolean closed = true;
	long kSz = 0;
	long ram = 0;
	private long maxSz = 0;
	private int hashRoutes = 0;
	// TODO change the kBufMazSize so it not reflective to the pageSize
	private static final int kBufMaxSize = 10485760 / Main.chunkStorePageSize;
	private final BitSet freeSlots = new BitSet();
	private boolean firstGCRun = true;
	private boolean flushing = false;

	public void init(long maxSize, String fileName) throws IOException,
			HashtableFullException {
		if (Main.compressedIndex)
			maps = new ByteArrayLongMap[65535];
		else
			maps = new ByteArrayLongMap[256];
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
		int hashRoute = hashb;

		ByteArrayLongMap m = maps[hashRoute];
		if (m == null) {
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
			}
		}
		return m;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractHashesMap#getAllocatedRam()
	 */
	@Override
	public long getAllocatedRam() {
		return this.ram;
	}

	public boolean isClosed() {
		return this.closed;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractHashesMap#getSize()
	 */
	@Override
	public long getSize() {
		return this.kSz;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractHashesMap#getUsedSize()
	 */
	@Override
	public long getUsedSize() {

		return kSz * Main.CHUNK_LENGTH;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractHashesMap#getMaxSize()
	 */
	@Override
	public long getMaxSize() {
		return this.size;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractHashesMap#claimRecords()
	 */
	@Override
	public synchronized void claimRecords() throws IOException {
		if (this.isClosed())
			throw new IOException("Hashtable " + this.fileName + " is close");
		SDFSLogger.getLog().info("claiming records");
		long startTime = System.currentTimeMillis();
		int z = 0;
		int k = 0;
		ByteBuffer lBuf = ByteBuffer.allocateDirect(8);
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
							long pos = (((long) val / (long) Main.chunkStorePageSize) * (long) ChunkData.RAWDL)
									+ ChunkData.CLAIMED_OFFSET;
							z++;
							lBuf.clear();
							lBuf.putLong(System.currentTimeMillis());
							lBuf.flip();
							kFc.write(lBuf, pos);
						}
					} catch (Exception e) {
						SDFSLogger.getLog().warn(
								"Unable to get claimed value for map " + i, e);
					} finally {
						this.iolock.unlock();
					}

				}
			} catch (NullPointerException e) {

			}
		}
		try {
			kFc.force(false);
		} catch (Exception e) {

		}
		SDFSLogger.getLog().info(
				"processed [" + z + "] claimed records in ["
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
		long endPos = 0;
		kRaf = new RandomAccessFile(fileName, this.fileParams);
		// kRaf.setLength(ChunkMetaData.RAWDL * size);
		kFc = (FileChannelImpl) kRaf.getChannel();
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
						} catch (Exception e) {
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
										if (value > endPos)
											endPos = value + Main.CHUNK_LENGTH;
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
		System.out.println();
		HashChunkService.getChuckStore().setSize(endPos);
		SDFSLogger.getLog().info(
				"########## Finished Loading Hash Database in ["
						+ (System.currentTimeMillis() - start) / 100
						+ "] seconds ###########");
		SDFSLogger.getLog().info(
				"loaded [" + kSz + "] into the hashtable [" + this.fileName
						+ "] free slots available are [" + freeSl
						+ "] free slots added [" + this.freeSlots.cardinality()
						+ "] end file position is [" + endPos + "]!");

		return size;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractHashesMap#containsKey(byte[])
	 */
	@Override
	public boolean containsKey(byte[] key) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		return this.getMap(key).containsKey(key);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractHashesMap#getFreeBlocks()
	 */
	@Override
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
			ByteBuffer rbuf = ByteBuffer.allocateDirect(ChunkData.RAWDL);
			try {
				for (int i = 0; i < size; i++) {
					if (this.isClosed())
						break;
					byte[] raw = new byte[ChunkData.RAWDL];
					rbuf.clear();
					try {
						long fp = (long) i * (long) ChunkData.RAWDL;
						this.iolock.lock();
						int l = 0;
						try {
							l = this.kFc.read(rbuf, fp);
						} catch (Exception e) {
							SDFSLogger.getLog().debug(
									"error reading sdfs hash table while removing records at "
											+ fp);
						} finally {
							this.iolock.unlock();
						}
						if (l > 0) {

							rbuf.flip();
							rbuf.get(raw);
							if (!Arrays.equals(raw, BLANKCM)) {
								try {
									ChunkData cm = new ChunkData(raw);
									if (cm.getLastClaimed() < time) {
										if (this.remove(cm)) {
											rem++;
										}
									} else {
										cm = null;
									}
								} catch (Exception e1) {
									SDFSLogger.getLog().warn(
											"unable to access record at " + fp,
											e1);
								} finally {
									raw = null;
								}
							}
						}
					} catch (BufferUnderflowException e) {

					} finally {

					}

				}

			} catch (Exception e) {
				SDFSLogger.getLog().warn("unable to finish chunk removal", e);
			} finally {
				this.flushBuffer(true);
				// this.removingChunks = false;
				SDFSLogger.getLog().info(
						"Removed [" + rem + "] records. Free slots ["
								+ this.freeSlots.cardinality() + "]");

			}
			return rem;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.opendedup.collections.AbstractHashesMap#put(org.opendedup.sdfs.filestore
	 * .ChunkData)
	 */
	@Override
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
		cm =null;
		return added;
	}

	private ReentrantLock fslock = new ReentrantLock();

	private long getFreeSlot() {
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.opendedup.collections.AbstractHashesMap#put(org.opendedup.sdfs.filestore
	 * .ChunkData, boolean)
	 */
	@Override
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
		cm =null;
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
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		return false;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractHashesMap#get(byte[])
	 */
	@Override
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractHashesMap#getData(byte[])
	 */
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

	private void flushBuffer(boolean lock) throws IOException {
		List<ChunkData> oldkBuf = null;
		if (lock) {
			this.arlock.lock();
		}

		try {
			if (kBuf.size() == 0) {
				return;
			}

			oldkBuf = kBuf;
			if (this.isClosed())
				kBuf = null;
			else {
				kBuf = new ArrayList<ChunkData>(oldkBuf.size());
			}
		} finally {
			if (lock)
				this.arlock.unlock();
		}
		if (oldkBuf.size() > 0) {
			Iterator<ChunkData> iter = oldkBuf.iterator();
			this.iolock.lock();
			this.flushing = true;
			try {
				while (iter.hasNext()) {
					ChunkData cm = iter.next();
					if (cm != null) {
						long pos = (cm.getcPos() / (long) Main.chunkStorePageSize)
								* (long) ChunkData.RAWDL;
						try {
							kFc.write(cm.getMetaDataBytes(), pos);
						} catch (java.nio.channels.ClosedChannelException e1) {
							kFc = (FileChannelImpl) new RandomAccessFile(
									fileName, this.fileParams).getChannel();
							kFc.write(cm.getMetaDataBytes(), pos);
						} catch (Exception e) {
							SDFSLogger.getLog().error(
									"error while writing buffer", e);
						} finally {
							if (cm.ismDelete()) {
								this.addFreeSlot(cm.getcPos());
								this.kSz--;
							}
							cm = null;
						}
						cm = null;
					}
				}
				kFc.force(true);
			} catch (Exception e) {
				SDFSLogger.getLog().error("error while flushing buffers", e);
			} finally {
				this.flushing = false;
				this.iolock.unlock();
			}
			oldkBuf.clear();

		}
		oldkBuf = null;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.opendedup.collections.AbstractHashesMap#remove(org.opendedup.sdfs
	 * .filestore.ChunkData)
	 */
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
					this.kBuf.add(cm);
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractHashesMap#sync()
	 */
	@Override
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.collections.AbstractHashesMap#close()
	 */
	@Override
	public void close() {
		this.arlock.lock();
		this.iolock.lock();
		this.closed = true;
		try {
			this.flushBuffer(true);
			while (this.flushing) {
				Thread.sleep(100);
			}
			this.kFc.close();
			this.kFc = null;
			this.kRaf = null;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.arlock.unlock();
			this.iolock.unlock();
		}
		SDFSLogger.getLog().info("Hashtable [" + this.fileName + "] closed");
	}

	@Override
	public void vanish() throws IOException {
		// TODO Auto-generated method stub

	}
}
