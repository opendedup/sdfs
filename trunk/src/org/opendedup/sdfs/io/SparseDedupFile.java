package org.opendedup.sdfs.io;

import java.io.File;



import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendedup.collections.LargeLongByteArrayMap;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.DeleteDir;
import org.opendedup.util.HashFunctionPool;
import org.opendedup.util.ThreadPool;
import org.opendedup.util.VMDKParser;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap.Builder;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;

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
	private ReentrantLock flushingLock = new ReentrantLock();
	private ReentrantLock channelLock = new ReentrantLock();
	private ReentrantLock initLock = new ReentrantLock();
	private LargeLongByteArrayMap chunkStore = null;
	private int maxWriteBuffers = ((Main.maxWriteBuffers * 1024 * 1024) / Main.CHUNK_LENGTH) + 1;
	private transient HashMap<Long, WritableCacheBuffer> flushingBuffers = new HashMap<Long, WritableCacheBuffer>();
	private transient ConcurrentLinkedHashMap<Long, WritableCacheBuffer> writeBuffers = new 
	Builder<Long, WritableCacheBuffer>().concurrencyLevel(Main.writeThreads).initialCapacity(maxWriteBuffers+1)
	.maximumWeightedCapacity(maxWriteBuffers+1).listener(
			new EvictionListener<Long, WritableCacheBuffer>() {
				// This method is called just after a new entry has been
				// added
				public void onEviction(Long key,
						WritableCacheBuffer writeBuffer) {

					if (writeBuffer != null) {
						flushingLock.lock();
						try {
							flushingBuffers.put(key, writeBuffer);
						} catch (Exception e) {

							// TODO Auto-generated catch block
							e.printStackTrace();
						} finally {
							flushingLock.unlock();
						}

						pool.execute(writeBuffer);
					}

				}
			}
	
	).build();
	
	private boolean closed = true;
	static {
		File f = new File(Main.dedupDBStore);
		if (!f.exists())
			f.mkdirs();

	}

	public SparseDedupFile(MetaDataDedupFile mf) throws IOException {
		log.finer("dedup file opened for " + mf.getPath());
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
		DedupFileChannel ch = this.getChannel();
		this.writeCache();
		this.sync();
		try {
			SparseDedupFile _df = new SparseDedupFile(snapmf);
			_df.bdb.vanish();
			_df.chunkStore.vanish();
			_df.forceClose();
			bdb.copy(_df.getDatabasePath());
			chunkStore.copy(_df.chunkStorePath);
			return _df;
		} catch (Exception e) {
			log.log(Level.WARNING, "unable to clone file " + mf.getPath(), e);
			throw new IOException("unable to clone file " + mf.getPath(), e);
		} finally {
			ch.close();
			ch = null;
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
		this.channelLock.lock();
		try {
			this.forceClose();
			String filePath = Main.dedupDBStore + File.separator
					+ this.GUID.substring(0, 2) + File.separator + this.GUID;
			DedupFileStore.removeOpenDedupFile(this.mf);
			return DeleteDir.deleteDirectory(new File(filePath));
		} catch (Exception e) {

		} finally {
			this.channelLock.unlock();
		}
		return false;
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
		log.finer("Flushing Cache of for " + mf.getPath() + " of size "
				+ this.writeBuffers.size());
		Object[] buffers = this.writeBuffers.values().toArray();
		int z = 0;
		for (int i = 0; i < buffers.length; i++) {
			WritableCacheBuffer buf = (WritableCacheBuffer) buffers[i];
			this.writeCache(buf, true);
			z++;
		}
		log.finer("flushed " + z + " buffers from " + mf.getPath());
	}

	public void writeCache(WritableCacheBuffer writeBuffer,
			boolean removeWhenWritten) throws IOException {
		if (this.closed) {
			throw new IOException("file already closed");
		}

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
				if (writeBuffer.isPrevDoop() && !writeBuffer.isNewChunk())
					mf.getIOMonitor().removeDuplicateBlock();
			}
			if (writeBuffer.getFilePosition() == 0
					&& mf.getPath().endsWith(".vmdk")) {
				try {
					VMDKData data = VMDKParser.parserVMDKFile(writeBuffer
							.getChunk());
					if (data != null) {
						mf.setVmdk(true);
						log.finer(data.toString());
					}
				} catch (Exception e) {
					log.log(Level.WARNING, "Unable to parse vmdk header for  "
							+ mf.getPath(), e);
				}
			}
			try {
				this.updateMap(writeBuffer, hash, doop);
				if (doop && !writeBuffer.isPrevDoop())
					mf.getIOMonitor().addDulicateBlock();
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

				WritableCacheBuffer buf = null;
				this.flushingLock.lock();
				try {
					buf = this.flushingBuffers.remove(writeBuffer
							.getFilePosition());
				} catch (Exception e) {
				} finally {
					this.flushingLock.unlock();
				}
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
		if (this.closed) {
			this.forceClose();
			throw new IOException("file already closed");
		}
		try {
			updatelock.lock();
			long filePosition = writeBuffer.getFilePosition();
			SparseDataChunk chunk = null;
			if (mf.isDedup() || doop) {
				chunk = new SparseDataChunk(doop, hash, false, System
						.currentTimeMillis());
			} else {
				chunk = new SparseDataChunk(doop, hash, true, System
						.currentTimeMillis());
				this.chunkStore.put(filePosition, writeBuffer.getChunk());
			}
			bdb.put(filePosition, chunk.getBytes());
		} catch (Exception e) {
			log.log(Level.SEVERE, "unable to write " + hash + " closing " + mf.getPath(), e);
			this.forceClose();
			throw new IOException(e);
		} finally {
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
		if (this.closed) {
			throw new IOException("file already closed");
		}
		try {
			marshallock.lock();
			long chunkPos = this.getChuckPosition(position);
			WritableCacheBuffer writeBuffer = this.writeBuffers.get(chunkPos);
			if (writeBuffer != null && writeBuffer.isClosed()) {
				writeBuffer.open();
			} else if (writeBuffer == null) {
				this.flushingLock.lock();
				try {
					writeBuffer = this.flushingBuffers.remove(chunkPos);
				} catch (Exception e) {
				} finally {
					this.flushingLock.unlock();
				}
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
			writeBuffer.setPrevDoop(ck.isDoop());
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
		if (this.closed) {
			this.forceClose();
			throw new IOException("file already closed");
		}
		long chunkPos = this.getChuckPosition(position);
		DedupChunk readBuffer = this.writeBuffers.get(chunkPos);
		if (readBuffer == null) {
			this.flushingLock.lock();
			try {
				readBuffer = this.flushingBuffers.get(chunkPos);
			} catch (Exception e) {
			} finally {
				this.flushingLock.unlock();
			}
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
		if (this.closed) {
			throw new IOException("file already closed");
		}
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
		if (this.closed) {
			throw new IOException("file already closed");
		}
		if (Main.safeSync) {
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
			//this.bdb.sync();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#getChannel()
	 */
	public DedupFileChannel getChannel() throws IOException {
		channelLock.lock();
		try {
			if (this.isClosed() || this.buffers.size() == 0)
				this.initDB();
			DedupFileChannel channel = new DedupFileChannel(mf);
			this.buffers.add(channel);
			return channel;
		} catch (IOException e) {
			throw e;
		} finally {
			channelLock.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.annesam.sdfs.io.AbstractDedupFile#unRegisterChannel(com.annesam.sdfs
	 * .io.DedupFileChannel)
	 */
	public void unRegisterChannel(DedupFileChannel channel) {
		channelLock.lock();
		try {
			this.buffers.remove(channel);
			if (this.buffers.size() == 0 && Main.safeClose) {
				this.forceClose();
			}
		} catch (Exception e) {
		} finally {
			channelLock.unlock();
		}
	}
	
	public boolean hasOpenChannels() {
		channelLock.lock();
		try {
		if(this.buffers.size() > 0)
			return true;
		else return false;
		} catch (Exception e) {
			return false;
		} finally {
			channelLock.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#close()
	 */
	public void forceClose() {
		log.info("closing " + mf.getPath());
		this.initLock.lock();
		try {
			try {
				ArrayList<DedupFileChannel> al = new ArrayList<DedupFileChannel>();
				for (int i = 0; i < this.buffers.size(); i++) {
					al.add(this.buffers.get(i));
				}
				for (int i = 0; i < al.size(); i++) {
					al.get(i).close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				this.writeCache();
			} catch (Exception e) {
			}
			try {
				this.bdb.sync();
			} catch (Exception e) {
			}
			try {
				this.bdb.close();
			} catch (Exception e) {
			}
			this.bdb = null;
			this.closed = true;
			try {
				this.chunkStore.close();
			} catch (Exception e) {
			}
			mf.setDedupFile(this);
			mf.sync();
		} catch (Exception e) {
			e.printStackTrace();
			// log.log(Level.WARNING, "Unable to close database " + this.GUID,
			// e);
		} finally {
			DedupFileStore.removeOpenDedupFile(mf);
			bdb = null;
			chunkStore = null;
			this.closed = true;
			this.initLock.unlock();
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
		this.initLock.lock();
		try {
			if (this.isClosed()) {
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
				if (!directory.exists()) {
					directory.mkdirs();
				}
				this.chunkStore = new LargeLongByteArrayMap(chunkStorePath,
						(long) -1, Main.CHUNK_LENGTH);
				this.bdb = new LongByteArrayMap(1 + Main.hashLength + 1 + 8,
						this.databasePath);
				DedupFileStore.addOpenDedupFile(this);
				this.closed = false;
			}
		} catch (IOException e) {
			throw e;
		} finally {
			this.initLock.unlock();
		}

	}

	public void optimize(long length) {
		DedupFileChannel ch = null;
		try {
			ch = this.getChannel();
			if (this.closed) {
				throw new IOException("file already closed");
			}
			if (!mf.isDedup()) {
				this.writeCache();
				this.checkForDups();
				this.chunkStore.close();
				new File(this.chunkStorePath).delete();
				this.chunkStore = new LargeLongByteArrayMap(chunkStorePath,
						(long) -1, Main.CHUNK_LENGTH);
			} else
				this.pushLocalDataToChunkStore();
		} catch (IOException e) {

		} finally {
			try {
				ch.close();
			} catch (Exception e) {

			}
			ch = null;
		}
	}

	private void pushLocalDataToChunkStore() throws IOException {
		if (this.closed) {
			throw new IOException("file already closed");
		}
		bdb.iterInit();
		Long l = bdb.nextKey();
		log.finer("removing for dups within " + mf.getPath());
		long doops = 0;
		long records = 0;
		while (l > -1) {
			try {
				SparseDataChunk pck = new SparseDataChunk(bdb.get(l));
				WritableCacheBuffer writeBuffer = null;
				if (pck.isLocalData() && mf.isDedup()) {
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
		log.finer("Checked [" + records + "] blocks found [" + doops
				+ "] new duplicate blocks");
	}

	private void checkForDups() throws IOException {
		if (this.closed) {
			throw new IOException("file already closed");
		}
		bdb.iterInit();
		Long l = bdb.nextKey();
		log.finer("checking for dups");
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
		log.finer("Checked [" + records + "] blocks found [" + doops
				+ "] new duplicate blocks from [" + localRec
				+ "] local records last key was [" + pos + "]");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#lastModified()
	 */
	public long lastModified() throws IOException {
		return mf.lastModified();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#getHash(long, boolean)
	 */
	public synchronized DedupChunk getHash(long location, boolean create)
			throws IOException {
		if (this.closed) {
			throw new IOException("file already closed");
		}
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
					ck.setDoop(pck.isDoop());
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
		if (this.closed) {
			throw new IOException("file already closed");
		}
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
