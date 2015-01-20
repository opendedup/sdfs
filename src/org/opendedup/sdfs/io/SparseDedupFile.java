package org.opendedup.sdfs.io;

import java.io.File;


import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.opendedup.collections.DataArchivedException;
import org.opendedup.collections.DataMapInterface;
import org.opendedup.collections.HashtableFullException;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.hashing.AbstractHashEngine;
import org.opendedup.hashing.Finger;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.hashing.ThreadPool;
import org.opendedup.hashing.VariableHashEngine;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.cluster.BlockDevSocket;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;
import org.opendedup.sdfs.io.events.SFileDeleted;
import org.opendedup.sdfs.io.events.SFileWritten;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.DeleteDir;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.eventbus.EventBus;

public class SparseDedupFile implements DedupFile {

	private ArrayList<DedupFileLock> locks = new ArrayList<DedupFileLock>();
	private String GUID = "";
	private static EventBus eventBus = new EventBus();
	public transient MetaDataDedupFile mf;
	private transient final ArrayList<DedupFileChannel> channels = new ArrayList<DedupFileChannel>();
	private transient String databasePath = null;
	private transient String databaseDirPath = null;
	private DedupFileChannel staticChannel = null;
	public long lastSync = 0;
	public static boolean storageConnected = true;
	public DataMapInterface bdb = null;
	MessageDigest digest = null;
	public static final HashFunctionPool hashPool = new HashFunctionPool(
			Main.writeThreads + 1);
	protected static transient final ThreadPool pool = new ThreadPool(
			Main.writeThreads + 1,
			((Main.maxWriteBuffers * 1024 * 1024) / Main.CHUNK_LENGTH) * 2);
	private final ReentrantLock channelLock = new ReentrantLock();
	private final ReentrantLock syncLock = new ReentrantLock();
	private static int maxWriteBuffers = ((Main.maxWriteBuffers * 1024 * 1024) / Main.CHUNK_LENGTH) + 1;
	private transient final ConcurrentHashMap<Long, DedupChunkInterface> flushingBuffers = new ConcurrentHashMap<Long, DedupChunkInterface>(
			1024, .75f, Main.writeThreads * 3);
	private static transient BlockingQueue<Runnable> worksQueue = new LinkedBlockingQueue<Runnable>(
			HashFunctionPool.max_hash_cluster);
	private static transient RejectedExecutionHandler executionHandler = new BlockPolicy();
	private boolean deleted = false;
	protected static transient ThreadPoolExecutor executor = new ThreadPoolExecutor(
			Main.writeThreads, Main.writeThreads, 10, TimeUnit.SECONDS,
			worksQueue, executionHandler);
	private boolean dirty = false;
	protected boolean toOccured = false;
	protected boolean errOccured = false;
	public boolean isCopyExt;
	private boolean reconstructed = false;

	public static void registerListener(Object obj) {
		eventBus.register(obj);
	}

	private LoadingCache<Long, DedupChunkInterface> writeBuffers = CacheBuilder
			.newBuilder().maximumSize(maxWriteBuffers + 1)
			.concurrencyLevel(Main.writeThreads)
			.expireAfterAccess(10, TimeUnit.SECONDS)
			.removalListener(new RemovalListener<Long, DedupChunkInterface>() {
				public void onRemoval(
						RemovalNotification<Long, DedupChunkInterface> removal) {
					DedupChunkInterface ck = removal.getValue();
					try {
						ck.flush();
					} catch (BufferClosedException e) {
						SDFSLogger.getLog().error(
								"Error while closing buffer at "
										+ removal.getKey());
					}
					// flushingBuffers.put(pos, ck);
				}
			}).build(new CacheLoader<Long, DedupChunkInterface>() {
				public DedupChunkInterface load(Long key) throws IOException,
						FileClosedException {
					if (closed) {
						throw new FileClosedException("file already closed");
					}
					DedupChunkInterface writeBuffer = null;
					writeBuffer = flushingBuffers.remove(key);
					if (writeBuffer == null) {
						writeBuffer = marshalWriteBuffer(key);
					}
					writeBuffer.open();

					return writeBuffer;
				}

			});

	private boolean closed = true;
	static {
		File f = new File(Main.dedupDBStore);
		if (!f.exists())
			f.mkdirs();
	}

	public static synchronized void flushThreadPool() {
		pool.flush();
	}

	public SparseDedupFile(MetaDataDedupFile mf) throws IOException {
		// SDFSLogger.getLog().info("Using LRU Max WriteBuffers=" +
		// this.maxWriteBuffers);
		this.mf = mf;
		if (mf.getDfGuid() == null) {
			// new Instance
			this.GUID = UUID.randomUUID().toString();
		} else {
			this.GUID = mf.getDfGuid();
		}
		if (SDFSLogger.isDebug()) {
			SDFSLogger.getLog().debug(
					"dedup file opened for " + mf.getPath() + " df="
							+ this.GUID);
			SDFSLogger.getLog().debug("LRU Size is " + (maxWriteBuffers + 1));
		}
	}

	private DedupChunkInterface load(Long key) throws IOException,
			FileClosedException {
		if (closed) {
			throw new FileClosedException("file already closed");
		}
		long chunkPos = getChuckPosition(key);
		DedupChunkInterface writeBuffer = null;
		writeBuffer = flushingBuffers.remove(chunkPos);
		if (writeBuffer == null) {
			writeBuffer = marshalWriteBuffer(chunkPos);
		}

		writeBuffer.open();

		return writeBuffer;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seecom.annesam.sdfs.io.AbstractDedupFile#snapshot(com.annesam.sdfs.io.
	 * MetaDataDedupFile)
	 */
	@Override
	public DedupFile snapshot(MetaDataDedupFile snapmf) throws IOException,
			HashtableFullException {
		return this.snapshot(snapmf, true);
	}

	@Override
	public DedupFile snapshot(MetaDataDedupFile snapmf, boolean propigate)
			throws IOException, HashtableFullException {
		DedupFileChannel ch = null;
		DedupFileChannel _ch = null;
		SparseDedupFile _df = null;
		try {
			ch = this.getChannel(-1);
			this.sync(true);
			_df = new SparseDedupFile(snapmf);
			File _directory = new File(Main.dedupDBStore + File.separator
					+ _df.GUID.substring(0, 2) + File.separator + _df.GUID);
			File _dbf = new File(_directory.getPath() + File.separator
					+ _df.GUID + ".map");
			File _dbc = new File(_directory.getPath() + File.separator
					+ _df.GUID + ".chk");
			if (SDFSLogger.isDebug()) {
				SDFSLogger.getLog().debug("Snap folder is " + _directory);
				SDFSLogger.getLog().debug("Snap map is " + _dbf);
				SDFSLogger.getLog().debug("Snap chunk is " + _dbc);
			}
			bdb.copy(_dbf.getPath());

			snapmf.setDedupFile(_df);
			return _df;
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to clone file " + mf.getPath(), e);
			throw new IOException("unable to clone file " + mf.getPath(), e);
		} finally {
			if (ch != null)
				this.unRegisterChannel(ch, -1);
			if (_ch != null)
				_df.unRegisterChannel(_ch, -1);
			ch = null;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seecom.annesam.sdfs.io.AbstractDedupFile#snapshot(com.annesam.sdfs.io.
	 * MetaDataDedupFile)
	 */
	@Override
	public void copyTo(String path) throws IOException {
		this.copyTo(path, true);
	}

	@Override
	public void copyTo(String path, boolean propigate) throws IOException {
		DedupFileChannel ch = null;
		File dest = new File(path + File.separator + "ddb" + File.separator
				+ this.GUID.substring(0, 2) + File.separator + this.GUID);
		dest.mkdirs();
		try {
			ch = this.getChannel(-1);
			this.writeCache();
			this.sync(true);
			bdb.copy(dest.getPath() + File.separator + this.GUID + ".map");
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to copy to" + mf.getPath(), e);
			throw new IOException("unable to clone file " + mf.getPath(), e);
		} finally {
			if (ch != null) {
				this.unRegisterChannel(ch, -1);
			}
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

	@Override
	public boolean delete() {
		return this.delete(true);
	}

	@Override
	public boolean delete(boolean propigate) {
		this.channelLock.lock();
		this.syncLock.lock();
		try {
			this.deleted = true;
			this.forceClose();
			String filePath = Main.dedupDBStore + File.separator
					+ this.GUID.substring(0, 2) + File.separator + this.GUID;
			DedupFileStore.removeOpenDedupFile(this.GUID);
			eventBus.post(new SFileDeleted(filePath + File.separator
					+ this.GUID + ".map"));
			return DeleteDir.deleteDirectory(new File(filePath));
		} catch (Exception e) {

		} finally {
			this.channelLock.unlock();
			this.syncLock.unlock();
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#isClosed()
	 */
	@Override
	public boolean isClosed() {
		return this.closed;
	}

	public int writeCache() throws IOException, HashtableFullException {
		try {
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug(
						"Flushing Cache of for " + mf.getPath() + " of size "
								+ this.writeBuffers.size());
			this.writeBuffers.invalidateAll();
			int z = this.flushingBuffers.size();
			int i = 0;
			int x = 1;
			for (;;) {
				i++;
				if (this.flushingBuffers.size() == 0)
					return z;
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					SDFSLogger.getLog().warn("interrupted");
					break;
				}
				if (i > 30000) {
					int sec = (i / 1000) * x;
					SDFSLogger
							.getLog()
							.warn("WriteCache has take over [" + sec
									+ "] seconds. There are still "
									+ this.flushingBuffers.size() + " in flush");
					i = 0;
					x++;
				}
			}
		} finally {

		}
		return -1;
	}

	public void setMetaDataDedupFile(MetaDataDedupFile mf) {
		this.mf = mf;
	}

	@Override
	public void writeCache(WritableCacheBuffer writeBuffer) throws IOException,
			HashtableFullException, FileClosedException, DataArchivedException {
		if (this.closed) {
			throw new FileClosedException("file already closed");
		}
		if (writeBuffer == null)
			return;
		if (writeBuffer.isDirty()) {
			this.dirty = true;
			try {

				int dups = 0;
				if (writeBuffer.isBatchProcessed()) {
					for (HashLocPair p : writeBuffer.getFingers()) {
						if (!writeBuffer.isBatchwritten())
							p.hashloc = HCServiceProxy.writeChunk(p.hash,
									p.data, p.hashloc);
					}
				} else {
					if (HashFunctionPool.max_hash_cluster == 1) {
						HashLocPair p = null;
						if (writeBuffer.getFingers().size() == 0)
							p = new HashLocPair();
						else
							p = writeBuffer.getFingers().get(0);
						AbstractHashEngine hc = hashPool.borrowObject();
						try {
							p.hash = hc.getHash(writeBuffer.getFlushedBuffer());
						} catch (Exception e) {
							throw new IOException(e);
						} finally {
							hashPool.returnObject(hc);
						}
						byte[] b = writeBuffer.getFlushedBuffer();
						p.hashloc = HCServiceProxy.writeChunk(p.hash, b,
								mf.isDedup());
						if (p.hashloc[0] == 1)
							dups = writeBuffer.capacity();
						p.len = b.length;
						p.pos = 0;
						if (writeBuffer.getFingers().size() == 0)
							writeBuffer.getFingers().add(p);
						else
							writeBuffer.getFingers().set(0, p);

					} else {
						VariableHashEngine hc = (VariableHashEngine) hashPool
								.borrowObject();

						try {
							List<Finger> fs = hc.getChunks(writeBuffer
									.getFlushedBuffer());
							ArrayList<HashLocPair> ar = new ArrayList<HashLocPair>(
									fs.size());
							AsyncChunkWriteActionListener l = new AsyncChunkWriteActionListener() {

								@Override
								public void commandException(Finger result,
										Throwable e) {
									int _dn = this.incrementandGetDN();
									this.incrementAndGetDNEX();
									SDFSLogger.getLog().error(
											"Error while getting hash", e);
									if (_dn >= this.getMaxSz()) {
										synchronized (this) {
											this.notifyAll();
										}
									}
								}

								@Override
								public void commandResponse(Finger result) {
									int _dn = this.incrementandGetDN();
									if (_dn >= this.getMaxSz()) {
										synchronized (this) {
											this.notifyAll();
										}
									}
								}

								@Override
								public void commandArchiveException(
										DataArchivedException e) {
									this.incrementAndGetDNEX();
									this.dar = e;
									SDFSLogger.getLog().error(
											"Data has been archived", e);
									this.incrementandGetDN();

									synchronized (this) {
										this.notifyAll();
									}

								}

							};
							l.setMaxSize(fs.size());
							for (Finger f : fs) {
								f.l = l;
								f.dedup = mf.isDedup();
								executor.execute(f);
							}
							int wl = 0;
							int tm = 1000;

							int al = 0;
							while (l.getDN() < fs.size() && l.getDNEX() == 0) {
								if (al == 30) {
									int nt = wl / 1000;
									SDFSLogger
											.getLog()
											.debug("Slow io, waited ["
													+ nt
													+ "] seconds for all writes to complete.");
									al = 0;
								}
								if (Main.writeTimeoutSeconds > 0
										&& wl > (Main.writeTimeoutSeconds * tm)) {
									int nt = wl / 1000;
									this.toOccured = true;
									throw new IOException(
											"Write Timed Out after ["
													+ nt
													+ "] seconds. Expected ["
													+ fs.size()
													+ "] block writes but only ["
													+ l.getDN()
													+ "] were completed");
								}
								if (l.dar != null) {
									throw l.dar;
								}

								synchronized (l) {
									l.wait(tm);
								}
								al++;
								wl += tm;
							}
							if (l.getDN() < fs.size()) {
								this.toOccured = true;
								throw new IOException(
										"Write Timed Out expected ["
												+ fs.size() + "] but got ["
												+ l.getDN() + "]");
							}
							if (l.dar != null)
								throw l.dar;
							if (l.getDNEX() > 0) {
								this.errOccured = true;
								throw new IOException("Write Failed");
							}
							// SDFSLogger.getLog().info("broke data up into " +
							// fs.size() + " chunks");
							int _pos = 0;
							for (Finger f : fs) {
								HashLocPair p = new HashLocPair();
								try {
									p.hash = f.hash;
									p.hashloc = f.hl;
									p.len = f.len;
									p.offset = 0;
									p.nlen = f.len;
									p.pos = _pos;
									_pos += f.chunk.length;
									if (p.hashloc[0] == 1)
										dups = dups + f.len;
									ar.add(p);
								} catch (Exception e) {
									SDFSLogger.getLog().warn(
											"unable to write object finger", e);
									throw e;
									// SDFSLogger.getLog().info("this chunk size is "
									// + f.chunk.length);
								}
							}
							writeBuffer.setDoop(dups);
							writeBuffer.setAR(ar);
						} catch(DataArchivedException e) {
							throw e;
						}
						catch (Exception e) {
							this.errOccured = true;
							throw e;
						} finally {
							hashPool.returnObject(hc);
						}
					}

				}
				/*
				 * if (hashloc[1] == 0 && !Main.chunkStoreLocal) throw new
				 * IOException( "unable to write chunk hash location at 1 = " +
				 * hashloc[1]);
				 */
				mf.getIOMonitor().addVirtualBytesWritten(
						writeBuffer.capacity(), true);
				mf.getIOMonitor().addActualBytesWritten(
						writeBuffer.capacity()
								- (dups - writeBuffer.getPrevDoop()), true);
				mf.getIOMonitor().addDulicateData(
						(dups - writeBuffer.getPrevDoop()), true);
				this.updateMap(writeBuffer, dups);
			}catch(DataArchivedException e) {
				throw e;
			}
			catch (Exception e) {
				SDFSLogger.getLog().fatal(
						"unable to add chunk at position "
								+ writeBuffer.getFilePosition(), e);
				this.errOccured = true;
				throw new IOException(e);
			} finally {

			}

		} else if (writeBuffer.isHlAdded()) {

			this.dirty = true;
			this.updateMap(writeBuffer, writeBuffer.getPrevDoop());
		}

	}

	@Override
	public void updateMap(DedupChunkInterface writeBuffer, int doop)
			throws FileClosedException, IOException {
		this.updateMap(writeBuffer, doop, true);
	}

	// private ReentrantLock updatelock = new ReentrantLock();
	@Override
	public void updateMap(DedupChunkInterface writeBuffer, int doop,
			boolean propigate) throws FileClosedException, IOException {
		if (this.closed) {
			throw new FileClosedException("file already closed");
		}
		SparseDataChunk chunk = null;
		try {
			// updatelock.lock();
			long filePosition = writeBuffer.getFilePosition();
			if (this.bdb.getVersion() > 0) {
				chunk = new SparseDataChunk(doop, writeBuffer.getFingers(),
						false, this.bdb.getVersion());
				chunk.setRecontructed(chunk.isRecontructed());
				// SDFSLogger.getLog().info("Hash Size =" + hash.length +
				// " hashloc len = " + writeBuffer.getHashLoc().length);
			} else {
				if (mf.isDedup() || doop > 0) {
					chunk = new SparseDataChunk(doop, writeBuffer.getFingers(),
							false, this.bdb.getVersion());
				} else {
					chunk = new SparseDataChunk(doop, writeBuffer.getFingers(),
							true, this.bdb.getVersion());
				}
			}
			bdb.put(filePosition, chunk.getBytes());
		} catch (Exception e) {
			SDFSLogger.getLog().fatal(
					"unable to write " + writeBuffer.getFilePosition()
							+ " closing " + mf.getPath(), e);
			throw new IOException(e);
		} finally {
			chunk = null;
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#getWriteBuffer(long)
	 */

	public SparseDataChunk getSparseDataChunk(long pos) throws IOException,
			FileClosedException {
		if (pos > mf.length())
			return new SparseDataChunk();
		if (this.closed) {
			throw new FileClosedException("file already closed");
		}
		if (this.toOccured) {
			throw new IOException("timeout occured");
		}
		if (this.errOccured) {
			throw new IOException("write error occured");
		}
		SparseDataChunk pck = null;
		byte[] b = bdb.get(pos);
		if (b == null)
			pck = new SparseDataChunk();
		else
			pck = new SparseDataChunk(b, this.bdb.getVersion());
		if (pos + Main.CHUNK_LENGTH > mf.length())
			pck.len = (int) (mf.length() - pos);
		else
			pck.len = Main.CHUNK_LENGTH;
		return pck;
	}

	public int putSparseDataChunk(long pos, SparseDataChunk buf)
			throws FileClosedException, IOException {
		if (this.closed) {
			throw new FileClosedException("file already closed");
		}
		if (this.toOccured) {
			throw new IOException("timeout occured");
		}
		if (this.errOccured) {
			throw new IOException("write error occured");
		}
		long place = this.getChuckPosition(pos);
		if (place != pos)
			throw new IOException("Chunk request position request [" + pos
					+ "] does not align with block size [" + Main.CHUNK_LENGTH
					+ "] place = " + place);
		writeBuffers.invalidate(place);
		int z = 0;
		while (this.flushingBuffers.containsKey(pos)) {
			try {
				Thread.sleep(10);
				z++;
			} catch (InterruptedException e) {

			}
			if (z > 6000)
				throw new IOException("Waiting for flush timed out");
		}
		if (buf.getFingers().size() > LongByteArrayMap.MAX_ELEMENTS_PER_AR)
			SDFSLogger.getLog().error(
					"eeeks " + buf.getFingers().size() + " > "
							+ LongByteArrayMap.MAX_ELEMENTS_PER_AR);
		bdb.put(pos, buf.getBytes());
		long epos = pos + buf.len;
		if (epos > mf.length())
			mf.setLength(epos, false);
		this.reconstructed = true;
		return buf.len;
	}

	@Override
	public DedupChunkInterface getWriteBuffer(long position)
			throws IOException, FileClosedException {
		try {
			if (this.closed) {
				throw new FileClosedException("file already closed");
			}
			if (this.toOccured) {
				throw new IOException("timeout occured");
			}
			if (this.errOccured) {
				throw new IOException("write error occured");
			}
			if (!storageConnected)
				throw new IOException("storage offline");
			long chunkPos = this.getChuckPosition(position);
			try {
				if (Main.volume.isClustered()) {
					return this.load(chunkPos);
				} else {
					return this.writeBuffers.get(chunkPos);
				}
			} 
			catch (Exception e) {
				throw new IOException(e);
			}
		} finally {
		}
	}

	private DedupChunkInterface marshalWriteBuffer(long chunkPos)
			throws IOException, FileClosedException {
		DedupChunk ck = null;
		try {
			WritableCacheBuffer writeBuffer = null;
			ck = this.getHash(chunkPos, true);

			if (ck.isNewChunk()) {
				writeBuffer = new WritableCacheBuffer(chunkPos, ck.getLength(),
						this, ck.getFingers(), ck.getReconstructed());
				if (this.reconstructed)
					writeBuffer.setReconstructed(true);
			} else {
				writeBuffer = new WritableCacheBuffer(ck, this);
				writeBuffer.setPrevDoop(ck.getDoop());
				if (this.reconstructed)
					writeBuffer.setReconstructed(true);
			}
			// need to fix this

			return writeBuffer;
		} finally {
			ck = null;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#getNumberofChunks()
	 */
	@Override
	public long getNumberofChunks() throws IOException, FileClosedException {
		if (this.closed) {
			throw new FileClosedException();
		}
		try {
			long count = -1;
			return count;
		} catch (Exception e) {
			SDFSLogger.getLog().warn(
					"Table does not exist in database " + this.GUID, e);
		} finally {
		}
		return -1;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#sync()
	 */
	public void sync(boolean force) throws FileClosedException, IOException {
		this.sync(force, true);
	}

	@Override
	public void sync(boolean force, boolean propigate)
			throws FileClosedException, IOException {
		this.syncLock.lock();
		try {
			if (!storageConnected)
				throw new IOException("storage offline");
			if (this.closed) {
				return;

			}

			if (Main.safeSync) {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("sync " + mf.getPath());
				long tm = 0;
				long wt = 0;
				long st = 0;
				if (SDFSLogger.isDebug())
					tm = System.currentTimeMillis();
				long wsz = this.writeBuffers.size();
				int fsz = this.flushingBuffers.size();
				this.writeCache();
				if (SDFSLogger.isDebug())
					wt = System.currentTimeMillis() - tm;
				this.bdb.sync();
				if (SDFSLogger.isDebug())
					st = System.currentTimeMillis() - tm - wt;
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug(
							"Sync wb=[" + wsz + "] fb=[" + fsz
									+ "] write fush [" + wt + "] bd sync ["
									+ st + "]");
				HCServiceProxy.sync();
			}
			if (this.toOccured)
				throw new IOException("timeout occured");
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			this.syncLock.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#getChannel()
	 */
	@Override
	public DedupFileChannel getChannel(int flags) throws IOException {
		channelLock.lock();
		try {
			if (!storageConnected)
				throw new IOException("storage offline");
			if (this.toOccured)
				throw new IOException("timeout occured");
			if (!Main.safeClose || Main.blockDev) {
				if (this.staticChannel == null) {
					if (this.isClosed())
						this.initDB();
					this.staticChannel = new DedupFileChannel(this, -1);
				}
				return this.staticChannel;
			} else {

				if (this.isClosed() || this.channels.size() == 0)
					this.initDB();
				DedupFileChannel channel = new DedupFileChannel(this, flags);
				this.channels.add(channel);
				return channel;

			}
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
	@Override
	public void unRegisterChannel(DedupFileChannel channel, int flags) {
		if (Main.safeClose && !Main.blockDev) {
			channelLock.lock();
			try {

				if (channel.getFlags() == flags) {
					this.channels.remove(channel);
					channel.close(flags);
					if (this.channels.size() == 0) {
						this.forceClose();
					}
				} else {
					SDFSLogger.getLog().warn(
							"unregister of filechannel for ["
									+ this.mf.getPath()
									+ "] failed because flags mismatch flags ["
									+ flags + "!=" + channel.getFlags() + "]");
				}
			} catch (Exception e) {
			} finally {
				channelLock.unlock();
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.annesam.sdfs.io.AbstractDedupFile#unRegisterChannel(com.annesam.sdfs
	 * .io.DedupFileChannel)
	 */
	@Override
	public void registerChannel(DedupFileChannel channel) throws IOException {
		if (!storageConnected)
			throw new IOException("storage offline");
		if (this.toOccured)
			throw new IOException("timeout occured");
		if (!Main.safeClose) {
			channelLock.lock();
			try {
				if (this.staticChannel == null) {
					if (this.isClosed())
						this.initDB();
					this.staticChannel = channel;
				}
			} catch (Exception e) {
			} finally {
				channelLock.unlock();
			}
		} else {
			channelLock.lock();
			try {
				if (this.isClosed() || this.channels.size() == 0)
					this.initDB();
				this.channels.add(channel);
			} finally {
				channelLock.unlock();
			}
		}
	}

	@Override
	public boolean hasOpenChannels() {
		channelLock.lock();
		try {
			if (this.channels.size() > 0)
				return true;
			else
				return false;
		} catch (Exception e) {
			return false;
		} finally {
			channelLock.unlock();
		}
	}

	public int openChannelsSize() {
		channelLock.lock();
		try {
			return this.channels.size();
		} catch (Exception e) {
			return -1;
		} finally {
			channelLock.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#close()
	 */
	@Override
	public void forceClose() throws IOException {
		this.syncLock.lock();
		this.channelLock.lock();
		try {
			if (!this.closed) {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug(
							"Closing dedupfile [" + mf.getPath() + "] guid="
									+ this.GUID);
				if (Main.safeClose) {
					try {
						ArrayList<DedupFileChannel> al = new ArrayList<DedupFileChannel>();
						for (int i = 0; i < this.channels.size(); i++) {
							al.add(this.channels.get(i));
						}
						for (int i = 0; i < al.size(); i++) {
							al.get(i).close(al.get(i).getFlags());
						}
					} catch (Exception e) {
						SDFSLogger.getLog().error(
								"error closing " + mf.getPath(), e);
					}
				} else {
					try {
						this.staticChannel.forceClose();
						this.staticChannel = null;
					} catch (Exception e) {

					}
				}
				try {
					int nwb = this.writeCache();
					int twb = 0;
					while (nwb > 0) {
						twb += nwb;
						nwb = this.writeCache();
						if (SDFSLogger.isDebug())
							SDFSLogger.getLog().debug(
									"Flushing " + nwb + " buffers");
					}
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog()
								.debug("Flushed " + twb + " buffers");

				} catch (Exception e) {
					SDFSLogger.getLog().error(
							"unable to flush " + this.databasePath, e);
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

				if (!this.deleted) {
					try {
					MetaFileStore.getMF(mf.getPath()).setDedupFile(this);
					MetaFileStore.getMF(mf.getPath()).sync();
					eventBus.post(new SFileWritten(this));
					}catch(Exception e) {
						SDFSLogger.getLog().error("error while syncing file in close",e);
					}
				}
			}
			if (this.toOccured) {
				this.toOccured = false;
				throw new IOException("timeout occured");
			}
			if (this.errOccured) {
				this.errOccured = false;
				throw new IOException("write error occured");
			}
			if (!storageConnected)
				throw new IOException("storage offline");
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("Closed [" + mf.getPath() + "]");
		} finally {
			try {
				DedupFileStore.removeOpenDedupFile(this.GUID);
				bdb = null;
				this.closed = true;
				this.dirty = false;
			} catch (Exception e) {
			}
			this.channelLock.unlock();
			this.syncLock.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#getGUID()
	 */
	@Override
	public String getGUID() {
		return GUID;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#getMetaFile()
	 */
	@Override
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
	@Override
	public void removeLock(DedupFileLock lock) {
		this.removeLock(lock, true);
	}

	@Override
	public void removeLock(DedupFileLock lock, boolean propigate) {
		this.locks.remove(lock);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seecom.annesam.sdfs.io.AbstractDedupFile#addLock(com.annesam.sdfs.io.
	 * DedupFileChannel, long, long, boolean)
	 */
	@Override
	public DedupFileLock addLock(DedupFileChannel ch, long position, long len,
			boolean shared) throws IOException {
		return this.addLock(ch, position, len, shared, true);
	}

	@Override
	public DedupFileLock addLock(DedupFileChannel ch, long position, long len,
			boolean shared, boolean propigate) throws IOException {
		if (this.overLaps(position, len, shared))
			throw new IOException("Overlapping Lock requested");
		else {
			return new DedupFileLock(ch, position, len, shared);
		}
	}

	public String getDatabasePath() {
		return this.databasePath;
	}

	@Override
	public String getDatabaseDirPath() {
		return this.databaseDirPath;
	}

	private void initDB() throws IOException {
		this.syncLock.lock();
		try {
			if (!storageConnected)
				throw new IOException("storage offline");
			if (this.isClosed()) {
				File directory = new File(Main.dedupDBStore + File.separator
						+ this.GUID.substring(0, 2) + File.separator
						+ this.GUID);
				File dbf = new File(directory.getPath() + File.separator
						+ this.GUID + ".map");

				this.databaseDirPath = directory.getPath();
				this.databasePath = dbf.getPath();
				if (!directory.exists()) {
					directory.mkdirs();
				}
				if (mf.getDev() != null) {
					this.bdb = new BlockDevSocket(mf.getDev(),
							this.databasePath);
				} else
					this.bdb = new LongByteArrayMap(this.databasePath);

				this.closed = false;
			}
			DedupFileStore.addOpenDedupFiles(this);
		} catch (IOException e) {
			SDFSLogger.getLog().warn("error while opening db", e);
			throw e;
		} finally {
			this.syncLock.unlock();
		}

	}

	@Override
	public void optimize() throws HashtableFullException {
		throw new HashtableFullException("not implemented");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#lastModified()
	 */
	@Override
	public long lastModified() throws IOException {
		return mf.lastModified();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#getHash(long, boolean)
	 */
	@Override
	public DedupChunk getHash(long location, boolean create)
			throws IOException, FileClosedException {
		if (this.closed) {
			throw new FileClosedException("file already closed");
		}
		long place = this.getChuckPosition(location);
		DedupChunk ck = null;
		try {
			// this.addReadAhead(place);
			byte[] b = this.bdb.get(place);
			if (b != null) {
				SparseDataChunk pck = new SparseDataChunk(b,
						this.bdb.getVersion());
				// ByteString data = pck.getData();
				// boolean dataEmpty = !pck.isLocalData();
				// if (dataEmpty) {
				ck = new DedupChunk(place, Main.CHUNK_LENGTH, false,
						pck.getFingers(), pck.isRecontructed());
				// } else {
				// byte dk[] = chunkStore.get(place);
				// ck = new DedupChunk(dk, place,
				// Main.CHUNK_LENGTH, pck.getHashLoc(), pck.getLen(),
				// pck.getPos());
				// }
				ck.setDoop(pck.getDoop());
				pck = null;
			}
			b = null;
			if (ck == null && create == true) {
				return createNewChunk(place);
			} else {
				return ck;
			}
		} catch (Exception e) {
			SDFSLogger.getLog().warn(
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
	@Override
	public void removeHash(long location) throws IOException {
		this.removeHash(location, true);
	}

	@Override
	public void removeHash(long location, boolean propigate) throws IOException {
		if (this.closed) {
			throw new IOException("file already closed");
		}
		long place = this.getChuckPosition(location);
		try {
			this.writeCache();
			this.bdb.remove(place);
		} catch (Exception e) {
			SDFSLogger.getLog().warn(
					"unable to remove chunk at position " + place, e);
			throw new IOException(e);
		} finally {
		}
	}

	@Override
	public void truncate(long size) throws IOException {
		this.truncate(size, true);
	}

	@Override
	public void truncate(long size, boolean propigate) throws IOException {
		try {
			if (this.closed) {
				throw new IOException("file already closed");
			}

			this.writeCache();
			if (size == 0) {
				this.mf.getIOMonitor().clearAllCounters(true);
			}
			this.bdb.truncate(size);
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to truncate to " + size, e);
			throw new IOException(e);
		} finally {
		}

	}

	private DedupChunk createNewChunk(long location) {
		DedupChunk ck = new DedupChunk(location, Main.CHUNK_LENGTH, true,
				new ArrayList<HashLocPair>(), false);
		return ck;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.io.AbstractDedupFile#getChuckPosition(long)
	 */
	@Override
	public long getChuckPosition(long location) {
		long place = location / Main.CHUNK_LENGTH;
		place = place * Main.CHUNK_LENGTH;
		return place;
	}

	@Override
	public boolean isAbsolute() {
		return true;
	}

	public boolean isDirty() {
		return this.dirty;
	}

	@Override
	public void putBufferIntoFlush(DedupChunkInterface writeBuffer) {
		if (SDFSLogger.isDebug()) {
			if (this.flushingBuffers.containsKey(writeBuffer.getFilePosition())) {
				SDFSLogger.getLog().info("buffer already contains key");
			}
		}
		this.flushingBuffers.put(writeBuffer.getFilePosition(), writeBuffer);
	}

	@Override
	public void removeBufferFromFlush(DedupChunkInterface writeBuffer) {
		DedupChunkInterface _wb = this.flushingBuffers.remove(writeBuffer
				.getFilePosition());
		if (SDFSLogger.isDebug()) {
			if (_wb.hashCode() != writeBuffer.hashCode()) {
				SDFSLogger.getLog().info("on remove hashcodes are not equal");
			}
		}
	}

	@Override
	public void trim(long start, int len) throws IOException {
		this.bdb.trim(start, len);
	}

	ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock();

	public Lock getReadLock() {
		return globalLock.readLock();
	}

	public Lock getWriteLock() {
		return globalLock.writeLock();
	}

}
