package org.opendedup.collections;

import java.io.File;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.collections.threads.SyncThread;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.CommandLineProgressBar;
import org.opendedup.util.NextPrime;
import org.opendedup.util.StringUtils;

import sun.nio.ch.FileChannelImpl;

public class CSByteArrayLongMap implements AbstractMap, AbstractHashesMap {
	RandomAccessFile kRaf = null;
	FileChannelImpl kFc = null;
	private long size = 0;
	private final ReentrantLock arlock = new ReentrantLock();
	private final ReentrantLock iolock = new ReentrantLock();
	private byte[] FREE = new byte[HashFunctionPool.hashLength];
	private byte[] REMOVED = new byte[HashFunctionPool.hashLength];
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
	long compactKsz = 0;
	long ram = 0;
	private long maxSz = 0;
	// TODO change the kBufMazSize so it not reflective to the pageSize
	private static final int kBufMaxSize = 10485760 / Main.chunkStorePageSize;
	private boolean firstGCRun = true;
	private boolean flushing = false;
	private SyncThread sth = null;
	private boolean compacting = false;
	private SDFSEvent loadEvent = SDFSEvent.loadHashDBEvent(
			"Loading Hash Database", Main.mountEvent);
	private long endPos;

	@Override
	public void init(long maxSize, String fileName) throws IOException,
			HashtableFullException {
		if (Main.compressedIndex)
			maps = new ByteArrayLongMap[65535];
		else
			maps = new ByteArrayLongMap[256];
		this.size = (maxSize);
		this.maxSz = maxSize;
		this.fileName = fileName;
		FREE = new byte[HashFunctionPool.hashLength];
		REMOVED = new byte[HashFunctionPool.hashLength];
		Arrays.fill(FREE, (byte) 0);
		Arrays.fill(BLANKCM, (byte) 0);
		Arrays.fill(REMOVED, (byte) 1);
		this.setUp();
		this.closed = false;
		sth = new SyncThread(this);
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
					ram = ram + (sz * (HashFunctionPool.hashLength + 8));
					m = new ByteArrayLongMap(sz, (short) (FREE.length));
					maps[hashRoute] = m;
					// System.out.println("Creating map at " + hashb +
					// " for total of " + this.mapsCreated);
				}
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

	@Override
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
	public synchronized void claimRecords(SDFSEvent evt) throws IOException {
		if (this.isClosed())
			throw new IOException("Hashtable " + this.fileName + " is close");
		SDFSLogger.getLog().info("claiming records");
		SDFSEvent tEvt = SDFSEvent.claimInfoEvent(
				"Claiming Records [" + this.getSize() + "] from ["
						+ this.fileName + "]", evt);
		tEvt.maxCt = this.getSize();
		long startTime = System.currentTimeMillis();
		long timeStamp = startTime + 30 * 1000;
		int z = 0;
		ByteBuffer lBuf = ByteBuffer.allocateDirect(8);
		for (int i = 0; i < maps.length; i++) {
			try {
				maps[i].iterInit();
				long val = 0;
				while (val != -1 && !this.closed) {
					tEvt.curCt++;
					try {
						this.iolock.lock();
						val = maps[i].nextClaimedValue(true);
						if (val != -1) {
							long pos = ((val / Main.chunkStorePageSize) * ChunkData.RAWDL)
									+ ChunkData.CLAIMED_OFFSET;
							z++;
							lBuf.clear();
							lBuf.putLong(timeStamp);
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
			this.iolock.lock();
			kFc.force(false);
		} catch (Exception e) {

		} finally {
			this.iolock.unlock();
		}
		tEvt.endEvent("processed [" + z + "] claimed records");
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
		kRaf = new RandomAccessFile(fileName, this.fileParams);
		// kRaf.setLength(ChunkMetaData.RAWDL * size);
		kFc = (FileChannelImpl) kRaf.getChannel();
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
			long entries = kRaf.length() / ChunkData.RAWDL;
			this.loadEvent.maxCt = entries;
			CommandLineProgressBar bar = new CommandLineProgressBar(
					"Loading Hashes", entries, System.out);
			while (kFc.position() < kRaf.length()) {
				count++;
				if (count > 100000) {
					count = 0;
					bar.update(kFc.position() / ChunkData.RAWDL);
					this.loadEvent.curCt = kFc.position() / ChunkData.RAWDL;
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
							long value = cm.getcPos();
							if (cm.ismDelete()) {
								// SDFSLogger.getLog().debug("chunk is deleted");
								
								freeSl++;
							} else {
								boolean added = this.put(cm, false);
								/*
								 * SDFSLogger.getLog().debug( "added " +
								 * StringUtils.getHexString(cm .getHash()) +
								 * " position is " + cm.getcPos());
								 */
								if (added)
									this.kSz++;
								if (value > endPos)
									endPos = value + Main.CHUNK_LENGTH;
							}
						}
					}
				} catch (BufferUnderflowException e) {

				}
			}

			bar.finish();
		}
		System.out.println();
		SDFSLogger.getLog().info(
				"########## Finished Loading Hash Database in ["
						+ (System.currentTimeMillis() - start) / 100
						+ "] seconds ###########");
		SDFSLogger.getLog().info(
				"loaded [" + kSz + "] into the hashtable [" + this.fileName
						+ "] free slots available are [" + freeSl
						+ "] end file position is [" + endPos + "]!");
		this.loadEvent.endEvent("Finished Loading Hash Database in ["
				+ (System.currentTimeMillis() - start) / 100 + "] seconds");

		return size;
	}

	@Override
	public long endStartingPosition() {
		return this.endPos;
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

	@Override
	public synchronized long removeRecords(long time, boolean forceRun,
			SDFSEvent evt) throws IOException {
		SDFSLogger.getLog().info(
				"Garbage collection starting for records older than "
						+ new Date(time));
		SDFSEvent tEvt = SDFSEvent.removeInfoEvent(
				"Garbage collection older than " + new Date(time), evt);
		tEvt.maxCt = this.size;
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
			ByteBuffer rbuf = ByteBuffer.allocate(ChunkData.RAWDL);
			try {
				for (int i = 0; i < size; i++) {
					if (this.isClosed())
						break;
					byte[] raw = new byte[ChunkData.RAWDL];
					rbuf.clear();
					try {
						long fp = (long) i * (long) ChunkData.RAWDL;
						tEvt.curCt = i;
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
									if (cm.getLastClaimed() != 0
											&& cm.getLastClaimed() < time) {
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
								+ HCServiceProxy.getFreeBlocks() + "]");
				tEvt.endEvent("Removed [" + rem + "] records. Free slots ["
						+ HCServiceProxy.getFreeBlocks() + "]");

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
		cm = null;
		return added;
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
			if (!cm.recoverd) {
				cm.persistData(true);
			}
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
				//this.addFreeSlot(cm.getcPos());
				cm = null;
			}
		} else {
			added = this.getMap(cm.getHash()).put(cm.getHash(), cm.getcPos(),
					(byte) 1);
		}
		cm = null;
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
		if (!compacting)
			throw new IOException("cannot update unless compacting");
		try {
			if (this.get(cm.getHash()) != -10) {
				this.arlock.lock();
				try {
					cm.persistData(true);
					this.getMap(cm.getHash()).update(cm.getHash(), -10);
					this.kBuf.add(cm);
					this.compactKsz++;
				} finally {
					this.arlock.unlock();

				}
			}
		} catch (KeyNotFoundException e) {
			return false;
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
						long pos = (cm.getcPos() / Main.chunkStorePageSize)
								* ChunkData.RAWDL;
						if (cm.ismDelete())
							cm.setLastClaimed(0);
						else {
							cm.setLastClaimed(System.currentTimeMillis());
						}
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

	@Override
	public boolean remove(ChunkData cm) throws IOException {
		return this.remove(cm, true);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.opendedup.collections.AbstractHashesMap#remove(org.opendedup.sdfs
	 * .filestore.ChunkData)
	 */

	public boolean remove(ChunkData cm, boolean persist) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		// this.flushFullBuffer();
		try {
			if (cm.getHash().length == 0)
				return true;
			if (!this.getMap(cm.getHash()).remove(cm.getHash())) {
				cm.setmDelete(false);
				return false;
			} else {
				cm.setmDelete(true);
				if (persist) {
					this.iolock.lock();
					if (this.isClosed()) {
						throw new IOException("hashtable [" + this.fileName
								+ "] is close");
					}
					try {
						long pos = (cm.getcPos() / Main.chunkStorePageSize)
								* ChunkData.RAWDL;
						if (cm.ismDelete()) {
							cm.setLastClaimed(0);
						} else {
							cm.setLastClaimed(System.currentTimeMillis());
						}
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
								this.kSz--;
							}
							cm = null;
						}
						cm = null;
					} catch (Exception e) {
					} finally {

						this.iolock.unlock();
					}
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
		this.sth.close();
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
		vanish(true);
	}

	@Override
	public void vanish(boolean propigateEvent) throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public void initCompact() throws IOException {
		this.arlock.lock();
		this.iolock.lock();
		compacting = true;
		compactKsz = 0;
		try {
			this.sth.close();
			this.flushBuffer(true);
			while (this.flushing) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					break;
				}
			}

			this.kFc.close();
			this.kRaf.close();
			this.kFc = null;
			this.kRaf = null;
			this.fileName = this.fileName + ".new";
			File _fs = new File(fileName);
			boolean exists = new File(fileName).exists();
			if (exists)
				throw new IOException(this.fileName
						+ " exists please remove and try again");
			if (!_fs.getParentFile().exists()) {
				_fs.getParentFile().mkdirs();
			}
			kRaf = new RandomAccessFile(fileName, this.fileParams);
			// kRaf.setLength(ChunkMetaData.RAWDL * size);
			kFc = (FileChannelImpl) kRaf.getChannel();
			sth = new SyncThread(this);
		} finally {
			this.arlock.unlock();
			this.iolock.unlock();
		}
	}

	@Override
	public void commitCompact(boolean force) throws IOException {
		this.arlock.lock();
		this.iolock.lock();
		try {
			if (!force && this.compactKsz != (this.kSz)) {
				SDFSLogger.getLog().error(
						"compacting sizes are not the same records=" + this.kSz
								+ " compacted records=" + this.compactKsz);
				/*
				 * for (int i = 0; i < maps.length; i++) { maps[i].iterInit();
				 * long val = 0; while (val != -1 && !this.closed) { val =
				 * maps[i].nextClaimedValue(true); if (val != -10) {
				 * SDFSLogger.getLog().warn("missed value at " + val); } } }
				 */
				this.kFc.close();
				this.kRaf.close();
				this.kFc = null;
				this.kRaf = null;
				File _fs = new File(fileName);
				_fs.delete();
				SDFSLogger.getLog().error("rolled back compacting");
				throw new IOException("compacting failed because records="
						+ this.kSz + " compacted records=" + this.compactKsz);
			} else if (force && this.compactKsz != (this.kSz)) {
				SDFSLogger.getLog().warn(
						"compacting sizes are not the same records=" + this.kSz
								+ " compacted records=" + this.compactKsz);
				long diff = this.kSz - this.compactKsz;
				if (diff > 0)
					SDFSLogger.getLog().warn(
							"compacting is discarding " + diff + " records");
				if (diff < 0)
					SDFSLogger.getLog().warn(
							"compacting is adding " + (diff * -1) + " records");
			}
			this.flushBuffer(true);
			while (this.flushing) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					throw new IOException(e);
				}
			}

			compacting = false;
			this.kFc.close();
			this.kRaf.close();
			this.kFc = null;
			this.kRaf = null;
			String nfn = this.fileName.replaceAll(".new", "");
			File _nf = new File(nfn);
			_nf.renameTo(new File(nfn + ".old"));
			File f = new File(fileName);
			f.renameTo(new File(nfn));
			this.fileName = nfn;
			new File(nfn + ".old").delete();
			kRaf = new RandomAccessFile(fileName, this.fileParams);
			// kRaf.setLength(ChunkMetaData.RAWDL * size);
			kFc = (FileChannelImpl) kRaf.getChannel();
			sth = new SyncThread(this);
		} finally {
			compacting = false;
			this.arlock.unlock();
			this.iolock.unlock();
		}

	}

	@Override
	public void rollbackCompact() throws IOException {
		this.arlock.lock();
		this.iolock.lock();
		try {
			try {
				this.kFc.close();
			} catch (Exception e) {
			}
			try {
				this.kRaf.close();
			} catch (Exception e) {
			}
			this.kFc = null;
			this.kRaf = null;

			File _fs = new File(fileName);
			_fs.delete();
			SDFSLogger.getLog().error("rolled back compacting");
		} finally {
			compacting = false;
			this.arlock.unlock();
			this.iolock.unlock();
		}

	}

}
