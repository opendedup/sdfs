package org.opendedup.sdfs.io;

import java.io.File;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.Security;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.opendedup.collections.LargeLongByteArrayMap;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.DeleteDir;
import org.opendedup.util.HashFunctionPool;
import org.opendedup.util.HashFunctions;
import org.opendedup.util.ThreadPool;
import org.opendedup.util.VMDKParser;

import com.reardencommerce.kernel.collections.shared.evictable.ConcurrentLinkedHashMap;

public class SparseDedupFile implements DedupFile {

	private ArrayList<DedupFileLock> locks = new ArrayList<DedupFileLock>();
	private String GUID = "";
	private transient MetaDataDedupFile mf;
	private transient static Logger log = Logger.getLogger("sdfs");
	private transient ArrayList<DedupFileChannel> buffers = new ArrayList<DedupFileChannel>();
	private transient String databasePath = null;
	private transient String databaseDirPath = null;
	private transient String chunkStorePath = null;
	LongByteArrayMap bdb = null;
	MessageDigest digest = null;
	private static HashFunctionPool hashPool = new HashFunctionPool(
			Main.writeThreads + 1);
	// private transient ArrayList<PreparedStatement> insertStatements = new
	// ArrayList<PreparedStatement>();
	private static transient final ThreadPool pool = new ThreadPool(
			Main.writeThreads + 1, Main.writeThreads);
	private ReentrantLock lock = new ReentrantLock();
	private LargeLongByteArrayMap chunkStore = null;
	private transient HashMap<Long, WritableCacheBuffer> flushingBuffers = new HashMap<Long, WritableCacheBuffer>();
	private transient ConcurrentLinkedHashMap<Long, WritableCacheBuffer> writeBuffers = ConcurrentLinkedHashMap
			.create(
					ConcurrentLinkedHashMap.EvictionPolicy.LRU,
					Main.maxWriteBuffers + 1,
					Main.writeThreads,
					new ConcurrentLinkedHashMap.EvictionListener<Long, WritableCacheBuffer>() {
						// This method is called just after a new entry has been
						// added
						public void onEviction(Long key,
								WritableCacheBuffer writeBuffer) {
							try {
								if (writeBuffer != null) {
									lock.lock();
									flushingBuffers.put(key, writeBuffer);
									lock.unlock();
									pool.execute(writeBuffer);
								}
							} catch (Exception e) {
								if (lock.isLocked())
									lock.unlock();
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					});

	private boolean closed = true;
	static {
		File f = new File(Main.dedupDBStore);
		if (!f.exists())
			f.mkdirs();

	}

	public SparseDedupFile(MetaDataDedupFile mf) throws IOException {
		log.info("dedup file opened for " + mf.getPath());
		this.mf = mf;
		this.init();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seecom.annesam.sdfs.io.AbstractDedupFile#snapshot(com.annesam.sdfs.io.
	 * MetaDataDedupFile)
	 */
	public DedupFile snapshot(MetaDataDedupFile snapmf) throws IOException {
		if (this.closed)
			this.initDB();
		this.writeCache();
		this.sync();
		try {
			SparseDedupFile _df = new SparseDedupFile(snapmf);
			_df.bdb.vanish();
			_df.chunkStore.vanish();
			_df.close();
			bdb.copy(_df.getDatabasePath());
			chunkStore.copy(_df.chunkStorePath);
			return _df;
		} catch (Exception e) {
			log.log(Level.WARNING, "unable to clone file " + mf.getPath(), e);
			throw new IOException("unable to clone file " + mf.getPath(), e);
		} finally {

		}
	}

	public void createBlankFile(long len) throws IOException {
		try {
			long numChks = len / Main.CHUNK_LENGTH;
			for (int i = 0; i < numChks; i++) {
				int pos = i * Main.CHUNK_LENGTH;
				SparseDataChunk chunk = new SparseDataChunk(true,
						Main.blankHash, false, System.currentTimeMillis());
				bdb.put(pos, chunk.getBytes());
			}
			mf.setLength(len, true);
		} catch (Exception e) {
			log.log(Level.WARNING, "unable to create blank file "
					+ mf.getPath(), e);
			throw new IOException(
					"unable to create blank file " + mf.getPath(), e);
		} finally {
		}
	}

	private boolean overLaps(long pos, long sz, boolean sharedRequested) {
		for (int i = 0; i < locks.size(); i++) {
			DedupFileLock lock = locks.get(i);
			if (lock.overLaps(pos, sz) && lock.isValid()) {
				if (!lock.isShared() || !sharedRequested)
					return true;
			}
		}
		return false;
	}

	public boolean delete() {
		this.close();
		String filePath = Main.dedupDBStore + File.separator
				+ this.GUID.substring(0, 2) + File.separator + this.GUID;
		DedupFileStore.removeOpenDedupFile(this.mf);
		return DeleteDir.deleteDirectory(new File(filePath));

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#isClosed()
	 */
	public boolean isClosed() {
		return this.closed;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#writeCache()
	 */
	public void writeCache() throws IOException {
		if (this.isClosed())
			throw new IOException("file is closed");

		log.finer("Flushing Cache of for " + mf.getPath() + " of size "
				+ this.writeBuffers.size());
		Object[] buffers = this.writeBuffers.values().toArray();
		for (int i = 0; i < buffers.length; i++) {
			WritableCacheBuffer buf = (WritableCacheBuffer) buffers[i];
			try {
				this.writeCache(buf, true);
			} catch (IOException e) {

			}
		}
	}

	public void writeCache(WritableCacheBuffer writeBuffer,
			boolean removeWhenWritten) throws IOException {
		if (this.isClosed())
			this.initDB();
		if (writeBuffer != null && writeBuffer.isDirty()) {
			MessageDigest hc = hashPool.borrowObject();
			byte[] hash = null;
			try {
				hash = hc.digest(writeBuffer.getChunk());
			} catch (Exception e) {
				throw new IOException(e);
			} finally {
				hashPool.returnObject(hc);
			}

			boolean doop = HCServiceProxy.writeChunk(hash, writeBuffer
					.getChunk(), writeBuffer.getLength(), writeBuffer
					.capacity(), mf.isDedup());
			mf.getIOMonitor().addVirtualBytesWritten(
					writeBuffer.getCurrentLen());
			if (!doop) {
				mf.getIOMonitor().addActualBytesWritten(
						writeBuffer.getCurrentLen());
				mf.getIOMonitor().removeDuplicateBlock(
						writeBuffer.getCurrentLen());
			}
			if (this.closed)
				this.initDB();
			if (writeBuffer.getFilePosition() == 0
					&& mf.getPath().endsWith(".vmdk")) {
				try {
					VMDKData data = VMDKParser.parserVMDKFile(writeBuffer
							.getChunk());
					if (data != null) {
						mf.setVmdk(true);
						mf.setVmdkData(data);
						log.info(data.toString());
					}
				} catch (Exception e) {
					log.log(Level.WARNING, "Unable to parse vmdk header for  "
							+ mf.getPath(), e);
				}
			}
			try {
				this.updateMap(writeBuffer, hash, doop);
				if (doop)
					mf.getIOMonitor().addDulicateBlock(writeBuffer.getLength());
			} catch (IOException e) {
				log.log(Level.SEVERE, "unable to add chunk ["
						+ writeBuffer.getHash() + "] at position "
						+ writeBuffer.getFilePosition(), e);
				WritableCacheBuffer buf = this.flushingBuffers
						.remove(writeBuffer.getFilePosition());
				if (buf != null)
					buf.destroy();
				throw new IOException("unable to add chunk ["
						+ writeBuffer.getHash() + "] at position "
						+ writeBuffer.getFilePosition() + " because "
						+ e.toString());
			} finally {
				WritableCacheBuffer buf = this.flushingBuffers
						.remove(writeBuffer.getFilePosition());
				if (buf != null)
					buf.destroy();
			}

			if (removeWhenWritten) {
				this.writeBuffers.remove(writeBuffer.getFilePosition());
				writeBuffer.destroy();
			}

		}

	}

	private ReentrantLock updatelock = new ReentrantLock();

	private void updateMap(WritableCacheBuffer writeBuffer, byte[] hash,
			boolean doop) throws IOException {
		if (this.isClosed())
			this.initDB();
		try {
			updatelock.lock();
			long filePosition = writeBuffer.getFilePosition();
			SparseDataChunk chunk = null;
			if (mf.isDedup() || doop) {
				chunk = new SparseDataChunk(doop, hash, false, System
						.currentTimeMillis());
				/*
				 * if(!mf.isDedup()) { try { chunkStore.lockCollection();
				 * chunkStore.remove(filePosition, false); }catch(Exception e)
				 * {} finally { chunkStore.unlockCollection(); } }
				 */
			} else {
				chunk = new SparseDataChunk(doop, hash, true, System
						.currentTimeMillis());
				this.chunkStore.put(filePosition, writeBuffer.getChunk());
			}
			bdb.put(filePosition, chunk.getBytes());
		} catch (Exception e) {
			updatelock.unlock();
			log.log(Level.SEVERE, "upable to write " + hash, e);
			throw new IOException(e.toString());
		} finally {
			if (updatelock.isLocked())
				updatelock.unlock();
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#getWriteBuffer(long)
	 */
	private ReentrantLock marshallock = new ReentrantLock();

	public WritableCacheBuffer getWriteBuffer(long position) throws IOException {
		if (this.isClosed())
			this.initDB();
		try {
			marshallock.lock();
			long chunkPos = this.getChuckPosition(position);
			WritableCacheBuffer writeBuffer = this.writeBuffers.get(chunkPos);
			if (writeBuffer != null && writeBuffer.isClosed()) {
				writeBuffer.open();
			} else if (writeBuffer == null) {
				writeBuffer = this.flushingBuffers.remove(chunkPos);
				if (writeBuffer != null) {
					this.writeBuffers.put(chunkPos, writeBuffer);
					writeBuffer.open();
				}
			}
			if (writeBuffer == null) {
				writeBuffer = marshalWriteBuffer(chunkPos);
			}
			marshallock.unlock();
			this.writeBuffers.putIfAbsent(chunkPos, writeBuffer);
			return this.writeBuffers.get(chunkPos);
		} catch (IOException e) {
			if (marshallock.isLocked())
				marshallock.unlock();
			log.log(Level.SEVERE,
					"Unable to get block at position " + position, e);
			throw new IOException("Unable to get block at position " + position);
		}
	}

	private WritableCacheBuffer marshalWriteBuffer(long chunkPos)
			throws IOException {
		WritableCacheBuffer writeBuffer = null;
		DedupChunk ck = this.getHash(chunkPos, true);
		if (ck.isNewChunk()) {
			writeBuffer = new WritableCacheBuffer(ck.getHash(), chunkPos, ck
					.getLength(), this);
		} else {
			writeBuffer = new WritableCacheBuffer(ck, this);
		}
		// need to fix this

		return writeBuffer;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#getReadBuffer(long)
	 */
	public DedupChunk getReadBuffer(long position) throws IOException {
		if (this.isClosed())
			this.initDB();
		long chunkPos = this.getChuckPosition(position);
		DedupChunk readBuffer = this.writeBuffers.get(chunkPos);
		if (readBuffer == null) {
			readBuffer = this.flushingBuffers.get(chunkPos);
		}
		if (readBuffer == null) {
			DedupChunk ck = this.getHash(position, true);
			readBuffer = new ReadOnlyCacheBuffer(ck, this);
		}
		return readBuffer;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#getNumberofChunks()
	 */
	public long getNumberofChunks() throws IOException {
		if (this.closed)
			this.initDB();
		try {
			long count = -1;
			return count;
		} catch (Exception e) {
			log.log(Level.WARNING, "Table does not exist in database "
					+ this.GUID, e);
		} finally {
		}
		return -1;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#sync()
	 */
	public void sync() throws IOException {

		if (Main.safeSync) {
			if (this.closed)
				this.initDB();
			this.bdb.sync();
			try {
				log.finer("Flushing Cache of for " + mf.getPath() + " of size "
						+ this.writeBuffers.size());
				Object[] buffers = this.writeBuffers.values().toArray();
				for (int i = 0; i < buffers.length; i++) {
					WritableCacheBuffer buf = (WritableCacheBuffer) buffers[i];
					try {
						buf.sync();
					} catch (IOException e) {

					}
				}
			} catch (Exception e) {
				throw new IOException(e);
			} finally {

			}
		} else {
			if (this.closed)
				this.initDB();
			this.bdb.sync();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#getChannel()
	 */
	public DedupFileChannel getChannel() throws IOException {

		DedupFileChannel channel = new DedupFileChannel(mf);
		this.buffers.add(channel);
		return channel;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.annesam.sdfs.io.AbstractDedupFile#unRegisterChannel(com.annesam.sdfs
	 * .io.DedupFileChannel)
	 */
	public void unRegisterChannel(DedupFileChannel channel) {
		this.buffers.remove(channel);
		if (this.buffers.size() == 0 && Main.safeClose)
			this.close();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#close()
	 */
	public synchronized void close() {
		try {
			try {
				this.writeCache();
			} catch (Exception e) {
			}
			;
			try {
				this.bdb.sync();
			} catch (Exception e) {
			}
			;
			try {
				this.bdb.close();
			} catch (Exception e) {
			}
			;
			this.closed = true;
			try {
				this.chunkStore.close();
			} catch (Exception e) {
			}
			;

			mf.setDedupFile(this);
			mf.sync();
		} catch (Exception e) {
			log.log(Level.WARNING, "Unable to close database " + this.GUID, e);
		} finally {
			DedupFileStore.removeOpenDedupFile(mf);
			bdb = null;
			chunkStore = null;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#getGUID()
	 */
	public String getGUID() {
		return GUID;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#getMetaFile()
	 */
	public MetaDataDedupFile getMetaFile() {
		return this.mf;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.annesam.sdfs.io.AbstractDedupFile#removeLock(com.annesam.sdfs.io.
	 * DedupFileLock)
	 */
	public void removeLock(DedupFileLock lock) {
		this.locks.remove(lock);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seecom.annesam.sdfs.io.AbstractDedupFile#addLock(com.annesam.sdfs.io.
	 * DedupFileChannel, long, long, boolean)
	 */
	public DedupFileLock addLock(DedupFileChannel ch, long position, long len,
			boolean shared) throws IOException {
		if (this.overLaps(position, len, shared))
			throw new IOException("Overlapping Lock requested");
		else {
			return new DedupFileLock(ch, position, len, shared);
		}
	}

	private void init() throws IOException {
		if (mf.getDfGuid() == null) {
			// new Instance
			this.GUID = UUID.randomUUID().toString();
		} else {
			this.GUID = mf.getDfGuid();
		}
		try {
			if (bdb == null)
				this.initDB();
		} catch (Exception e) {
			throw new IOException(e.toString());
		}
	}

	public String getDatabasePath() {
		return this.databasePath;
	}

	public String getDatabaseDirPath() {
		return this.databaseDirPath;
	}

	private synchronized void initDB() throws IOException {
		if (this.isClosed()) {
			try {
				File directory = new File(Main.dedupDBStore + File.separator
						+ this.GUID.substring(0, 2) + File.separator
						+ this.GUID);
				File dbf = new File(directory.getPath() + File.separator
						+ this.GUID + ".map");
				File dbc = new File(directory.getPath() + File.separator
						+ this.GUID + ".chk");
				this.databaseDirPath = directory.getPath();
				this.databasePath = dbf.getPath();
				this.chunkStorePath = dbc.getPath();
				// bdb.setdfunit(1000000);
				if (!directory.exists()) {
					directory.mkdirs();
				}
				this.chunkStore = new LargeLongByteArrayMap(dbc.getPath(),
						(long) -1, Main.CHUNK_LENGTH);
				this.bdb = new LongByteArrayMap(1 + Main.hashLength + 1 + 8,
						this.databasePath);
				// bdb.tune(1000000, -1, -1, 0);
				DedupFileStore.addOpenDedupFile(this);
			} catch (Exception except) {
				except.printStackTrace();
			}
			this.closed = false;
		}
	}

	public void optimize(long length) {
		try {
			if (this.closed)
				this.initDB();
			if (!mf.isDedup())
				this.checkForDups();
			else
				this.pushLocalDataToChunkStore();
		} catch (IOException e) {

		}
	}

	private void pushLocalDataToChunkStore() throws IOException {
		if (bdb == null)
			throw new IOException("bdb is null");
		bdb.iterInit();
		Long l = bdb.nextKey();
		log.info("checking for dups");
		long doops = 0;
		long records = 0;
		while (l > -1) {
			try {
				SparseDataChunk pck = new SparseDataChunk(bdb.get(l));
				WritableCacheBuffer writeBuffer = null;
				if (pck.isLocalData()) {
					byte[] chunk = chunkStore.get(l);
					boolean doop = HCServiceProxy.writeChunk(pck.getHash(),
							chunk, Main.CHUNK_LENGTH, Main.CHUNK_LENGTH, mf
									.isDedup());

					DedupChunk ck = new DedupChunk(pck.getHash(), chunk, l,
							Main.CHUNK_LENGTH);
					writeBuffer = new WritableCacheBuffer(ck, this);
					this.updateMap(writeBuffer, writeBuffer.getHash(), doop);
					ck = null;
					chunk = null;
					if (doop)
						doops++;
				}
				pck = null;
				writeBuffer = null;
				records++;
				l = bdb.nextKey();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		log.info("Checked [" + records + "] blocks found [" + doops
				+ "] new duplicate blocks");
	}

	private void checkForDups() throws IOException {
		if (bdb == null)
			throw new IOException("bdb is null");
		bdb.iterInit();
		Long l = bdb.nextKey();
		log.info("checking for dups");
		long doops = 0;
		long records = 0;
		long localRec = 0;
		long pos = 0;
		LargeLongByteArrayMap newCS = new LargeLongByteArrayMap(
				this.chunkStorePath + ".new", (long) -1, Main.CHUNK_LENGTH);
		this.chunkStore.lockCollection();
		try {
			while (l > -1) {
				pos = l;
				SparseDataChunk pck = new SparseDataChunk(bdb.get(l));
				if (pck.isLocalData()) {
					boolean doop = HCServiceProxy.hashExists(pck.getHash());
					if (doop) {
						doops++;
						pck.setLocalData(false);
						this.bdb.put(l, pck.getBytes());
					} else {
						newCS.put(l, chunkStore.get(l, false));
					}
					localRec++;
				}
				pck = null;
				records++;
				l = bdb.nextKey();
			}
			newCS.close();
			newCS.move(this.chunkStorePath);
			newCS = null;
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			this.chunkStore.unlockCollection();
		}
		log.info("Checked [" + records + "] blocks found [" + doops
				+ "] new duplicate blocks from [" + localRec
				+ "] local records last key was [" + pos + "]");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#lastModified()
	 */
	public long lastModified() {
		return mf.lastModified();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#getHash(long, boolean)
	 */
	public synchronized DedupChunk getHash(long location, boolean create)
			throws IOException {
		if (this.isClosed())
			this.initDB();
		long place = this.getChuckPosition(location);
		DedupChunk ck = null;
		try {
			// this.addReadAhead(place);
			byte[] b = this.bdb.get(place);
			if (b != null) {
				SparseDataChunk pck = new SparseDataChunk(b);
				// ByteString data = pck.getData();
				boolean dataEmpty = !pck.isLocalData();
				if (dataEmpty) {
					ck = new DedupChunk(pck.getHash(), place,
							Main.CHUNK_LENGTH, false);
				} else {
					byte dk[] = chunkStore.get(place);
					ck = new DedupChunk(pck.getHash(), dk, place,
							Main.CHUNK_LENGTH);
				}
				pck = null;

			}
			b = null;
			if (ck == null && create == true) {
				return createNewChunk(place);
			} else {
				return ck;
			}
		} catch (Exception e) {
			log.log(Level.WARNING,
					"unable to fetch chunk at position " + place, e);

			return null;
		} finally {

		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#removeHash(long)
	 */
	public synchronized void removeHash(long location) throws IOException {
		if (this.closed)
			this.initDB();
		long place = this.getChuckPosition(location);
		try {
			this.bdb.remove(place);
			this.chunkStore.remove(place);
		} catch (Exception e) {
			log.log(Level.WARNING, "unable to remove chunk at position "
					+ place, e);
			throw new IOException("unable to remove chunk at position " + place
					+ " because " + e.toString());
		} finally {

		}
	}

	private DedupChunk createNewChunk(long location) {
		DedupChunk ck = new DedupChunk(new byte[16], location,
				Main.CHUNK_LENGTH, true);
		return ck;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#getChuckPosition(long)
	 */
	public long getChuckPosition(long location) {
		long place = location / Main.CHUNK_LENGTH;
		place = place * Main.CHUNK_LENGTH;
		return place;
	}

	public boolean isAbsolute() {
		return true;
	}

}
