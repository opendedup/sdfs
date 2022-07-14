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
package org.opendedup.sdfs.io;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.opendedup.collections.ByteArrayWrapper;
import org.opendedup.collections.DataArchivedException;
import org.opendedup.collections.DataMapInterface;
import org.opendedup.collections.HashtableFullException;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.SparseDataChunk;
import org.opendedup.hashing.AbstractHashEngine;
import org.opendedup.hashing.Finger;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.filestore.HashBlobArchive;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.events.DataHashed;
import org.opendedup.sdfs.io.events.SFileClosed;
import org.opendedup.sdfs.io.events.SFileDeleted;
import org.opendedup.sdfs.io.events.SFileWritten;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.DeleteDir;
import org.opendedup.util.OSValidator;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.eventbus.EventBus;

public class SparseDedupFile implements DedupFile {

	private ArrayList<DedupFileLock> locks = new ArrayList<DedupFileLock>();
	private String GUID = "";
	private static EventBus eventBus = new EventBus();
	public transient MetaDataDedupFile mf;
	private transient final Set<DedupFileChannel> channels = new HashSet<DedupFileChannel>();
	private transient String databasePath = null;
	private transient String databaseDirPath = null;
	private DedupFileChannel staticChannel = null;
	public long lastSync = 0;
	public DataMapInterface bdb = null;
	MessageDigest digest = null;
	private final ReentrantLock syncLock = new ReentrantLock();
	private static int maxWriteBuffers = 2;
	protected transient final ConcurrentHashMap<Long, WritableCacheBuffer> flushingBuffers = new ConcurrentHashMap<Long, WritableCacheBuffer>(
			256, .75f);
	private static transient BlockingQueue<Runnable> worksQueue = new SynchronousQueue<Runnable>();
	private boolean deleted = false;
	protected static transient ThreadPoolExecutor executor = null;
	private boolean dirty = false;
	protected boolean toOccured = false;
	protected boolean errOccured = false;
	public boolean isCopyExt;
	private boolean reconstructed = false;

	public static AbstractHashEngine eng = HashFunctionPool.getHashEngine();
	private ConcurrentHashMap<Long, WritableCacheBuffer> openBuffers = new ConcurrentHashMap<Long, WritableCacheBuffer>(
			256, .75f);
	protected LoadingCache<Long, WritableCacheBuffer> writeBuffers = CacheBuilder.newBuilder()
			.maximumSize(maxWriteBuffers).expireAfterAccess(60, TimeUnit.SECONDS).concurrencyLevel(Main.writeThreads)
			.removalListener(new RemovalListener<Long, WritableCacheBuffer>() {
				public void onRemoval(RemovalNotification<Long, WritableCacheBuffer> removal) {
					WritableCacheBuffer ck = removal.getValue();
					try {
						ck.flush();
					} catch (BufferClosedException e) {
						SDFSLogger.getLog().warn("Error while closing buffer at " + removal.getKey());
					} catch (Exception e) {
						SDFSLogger.getLog().warn("unable to flush", e);
					}
				}
			}).build(new CacheLoader<Long, WritableCacheBuffer>() {
				public WritableCacheBuffer load(Long key) throws IOException, FileClosedException {

					if (closed) {
						throw new FileClosedException("file already closed");
					}
					WritableCacheBuffer writeBuffer = null;
					writeBuffer = flushingBuffers.get(key);
					if (writeBuffer == null) {
						writeBuffer = openBuffers.get(key);
					}
					if (writeBuffer == null) {
						writeBuffer = marshalWriteBuffer(key);
					}
					writeBuffer.open();
					return writeBuffer;
				}

			});
	static {
		File f = new File(Main.dedupDBStore);
		if (!f.exists())
			f.mkdirs();
		try {
			if (OSValidator.isWindows()) {
				Files.setAttribute(Paths.get(f.getParentFile().getPath()), "dos:hidden", true,
						LinkOption.NOFOLLOW_LINKS);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(2);
		}
		SDFSLogger.getLog().info("Maximum Write Buffers are " + maxWriteBuffers);

		executor = new ThreadPoolExecutor(1, Main.writeThreads, 10, TimeUnit.SECONDS, worksQueue,
				new ThreadPoolExecutor.CallerRunsPolicy());
	}

	public static void registerListener(Object obj) {
		eventBus.register(obj);

	}

	private boolean closed = true;
	static {

	}

	public SparseDedupFile(MetaDataDedupFile mf) throws IOException {
		// SDFSLogger.getLog().info("Using LRU Max WriteBuffers=" +
		// this.maxWriteBuffers);
		this.mf = mf;
		if (mf.getDfGuid() == null) {
			// new Instance
			this.GUID = UUID.randomUUID().toString();
			mf.setDfGuid(this.GUID);
		} else {
			this.GUID = mf.getDfGuid();
		}
		SDFSLogger.getLog().debug("dedup file opened for " + mf.getPath() + " df=" + this.GUID);
		SDFSLogger.getLog().debug("LRU Size is " + (maxWriteBuffers + 1));
	}

	public void setReconstructed(boolean reconstructed) {
		this.reconstructed = reconstructed;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @seecom.annesam.sdfs.io.AbstractDedupFile#snapshot(com.annesam.sdfs.io.
	 * MetaDataDedupFile)
	 */
	@Override
	public DedupFile snapshot(MetaDataDedupFile snapmf) throws IOException, HashtableFullException {
		return this.snapshot(snapmf, true);
	}

	public void CacheCleanup() {
		this.writeBuffers.cleanUp();
	}

	public CacheStats getCacheStats() {
		return this.writeBuffers.stats();
	}

	@Override
	public DedupFile snapshot(MetaDataDedupFile snapmf, boolean propigate) throws IOException, HashtableFullException {
		DedupFileChannel ch = null;
		DedupFileChannel _ch = null;
		SparseDedupFile _df = null;
		try {
			ch = this.getChannel(-1);
			this.sync(true);
			_df = new SparseDedupFile(snapmf);
			File _directory = new File(
					Main.dedupDBStore + File.separator + _df.GUID.substring(0, 2) + File.separator + _df.GUID);
			File _dbf = new File(_directory.getPath() + File.separator + _df.GUID + ".map");
			File _dbc = new File(_directory.getPath() + File.separator + _df.GUID + ".chk");
			SDFSLogger.getLog().debug("Snap folder is " + _directory);
			SDFSLogger.getLog().debug("Snap map is " + _dbf);
			SDFSLogger.getLog().debug("Snap chunk is " + _dbc);
			bdb.copy(_dbf.getPath(), true);

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
		File dest = new File(path + File.separator + "ddb" + File.separator + this.GUID.substring(0, 2) + File.separator
				+ this.GUID);
		dest.mkdirs();
		try {
			ch = this.getChannel(-1);
			this.writeCache();
			this.sync(true);
			bdb.copy(dest.getPath() + File.separator + this.GUID + ".map", true);
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
	public boolean delete(boolean localOnly) {
		this.syncLock.lock();
		try {
			this.deleted = true;
			if (Main.refCount) {

				File directory = new File(
						Main.dedupDBStore + File.separator + this.GUID.substring(0, 2) + File.separator + this.GUID);
				File dbf = new File(directory.getPath() + File.separator + this.GUID + ".map");
				File zdbf = new File(directory.getPath() + File.separator + this.GUID + ".map.lz4");
				if (dbf.exists() || zdbf.exists()) {
					if (bdb == null || bdb.isClosed()) {

						this.bdb = LongByteArrayMap.getMap(this.GUID);
					}
					if (Main.DDB_TRASH_ENABLED) {
						String ext = ".map";
						if (!dbf.exists()) {
							ext = ".map.lz4";
						}
						File dest = new File(Main.dedupDBTrashStore + File.separator + "ddb" + File.separator
								+ this.GUID.substring(0, 2) + File.separator + this.GUID + File.separator + this.GUID
								+ ext);
						this.bdb.copy(dest.getPath(), false);
						this.bdb.vanish(false);
						this.bdb.close();
						LongByteArrayMap.getMap(dest).close();
					} else {
						this.bdb.vanish(Main.refCount);
						this.bdb.close();
					}

				}
			}
			this.forceClose();

			String filePath = Main.dedupDBStore + File.separator + this.GUID.substring(0, 2) + File.separator
					+ this.GUID;

			DedupFileStore.removeOpenDedupFile(this.GUID);
			if (!localOnly)
				eventBus.post(new SFileDeleted(this));

			return DeleteDir.deleteDirectory(new File(filePath));

		} catch (Exception e) {
			SDFSLogger.getLog().warn("error in delete " + this.GUID, e);
		} finally {
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
				SDFSLogger.getLog()
						.debug("Flushing Cache of for " + mf.getPath() + " of size " + this.writeBuffers.size());
			this.writeBuffers.invalidateAll();
			int z = 0;
			synchronized (flushingBuffers) {
				z = this.flushingBuffers.size();
			}
			int i = 0;
			int x = 1;
			for (;;) {
				i++;
				synchronized (flushingBuffers) {
					if (this.flushingBuffers.size() == 0)
						return z;
				}
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					SDFSLogger.getLog().warn("interrupted");
					break;
				}
				if (i > 120000) {
					int sec = (i / 1000) * x;
					SDFSLogger.getLog().warn("WriteCache has take over [" + sec + "] seconds. There are still "
							+ this.flushingBuffers.size() + " in flush");
					for (WritableCacheBuffer buf : this.flushingBuffers.values()) {
						if (buf != null) {
							SDFSLogger.getLog().debug("closed=" + buf.closed + " flushing=" + buf.flushing + " pos="
									+ buf.getFilePosition());
							buf.lobj.lock();
							try {
								buf.close();
							} catch (Exception e) {
								SDFSLogger.getLog().warn("unable to close " + buf.getFilePosition(), e);
							} finally {
								buf.lobj.unlock();
							}
						}

					}
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
	public int hashCode() {
		return this.GUID.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		return this.GUID.equals(o.toString());
	}

	@Override
	public String toString() {
		return this.GUID;
	}

	@Override
	public void writeCache(WritableCacheBuffer writeBuffer)
			throws IOException, HashtableFullException, FileClosedException, DataArchivedException {
		try {
			if (this.closed) {
				throw new FileClosedException("file already closed");
			}
			if (writeBuffer.isDirty()) {

				this.dirty = true;
				try {
					boolean allInserted = false;
					int dups = 0;
					int retries = 0;
					while (!allInserted) {

						try {
							List<Finger> fs = null;
							fs = eng.getChunks(writeBuffer.getFlushedBuffer(), this.GUID);
							DataHashed dh = new DataHashed(this.mf, this, fs);
							eventBus.post(dh);
							HashMap<ByteArrayWrapper, Finger> mp = new HashMap<ByteArrayWrapper, Finger>();
							for (Finger f : fs) {
								ByteArrayWrapper ba = new ByteArrayWrapper(f.hash);
								Finger _f = mp.get(ba);
								if (_f == null) {
									f.claims = 1;
									mp.put(ba, f);
								} else {
									_f.claims++;
									mp.put(ba, _f);
								}
							}

							TreeMap<Integer, HashLocPair> ar = new TreeMap<Integer, HashLocPair>();

							AsyncChunkWriteActionListener l = new AsyncChunkWriteActionListener() {
								@Override
								public void commandException(Finger result, Throwable e) {
									int _dn = this.incrementandGetDN();
									this.incrementAndGetDNEX();
									SDFSLogger.getLog().error("Error while getting hash", e);
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
								public void commandArchiveException(DataArchivedException e) {
									this.incrementAndGetDNEX();
									this.dar = e;
									SDFSLogger.getLog().error("Data has been archived", e);
									this.incrementandGetDN();

									synchronized (this) {
										this.notifyAll();
									}

								}

							};
							l.setMaxSize(mp.size());
							for (Finger f : mp.values()) {
								f.l = l;
								executor.execute(f);
							}
							int wl = 0;
							int tm = 1000;

							int al = 0;
							while (l.getDN() < mp.size() && l.getDNEX() == 0) {

								if (al == 30) {
									int nt = wl / 1000;
									SDFSLogger.getLog()
											.debug("Slow io, waited [" + nt + "] seconds for all writes to complete.");
									al = 0;
								}
								if (Main.writeTimeoutSeconds > 0 && wl > (Main.writeTimeoutSeconds * tm)) {
									int nt = wl / 1000;
									this.toOccured = true;
									throw new IOException("Write Timed Out after [" + nt + "] seconds. Expected ["
											+ mp.size() + "] block writes but only [" + l.getDN() + "] were completed");
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
							if (l.getDN() < mp.size()) {
								this.toOccured = true;
								throw new IOException(
										"Write Timed Out expected [" + mp.size() + "] but got [" + l.getDN() + "]");
							}
							if (l.dar != null)
								throw l.dar;
							if (l.getDNEX() > 0) {
								this.errOccured = true;
								throw new IOException("Write Failed");
							}
							// SDFSLogger.getLog().info("broke data up into " +
							// fs.size() + " chunks");
							for (Finger f : fs) {
								HashLocPair p = new HashLocPair();
								try {
									ByteArrayWrapper ba = new ByteArrayWrapper(f.hash);
									Finger _f = mp.get(ba);
									p.hash = f.hash;
									p.hashloc = _f.hl.getHashLocs();
									p.len = f.len;
									p.offset = 0;
									p.nlen = f.len;
									p.pos = f.start;
									if (f.hl != null)
										p.setDup(!f.hl.getInserted());
									else
										p.setDup(true);
									if (p.isDup()) {
										dups += f.len;
									}
									ar.put(p.pos, p);
								} catch (Exception e) {
									SDFSLogger.getLog().warn("unable to write object finger", e);
									throw e;
									// SDFSLogger.getLog().info("this chunk size
									// is "
									// + f.chunk.length);
								}
							}
							writeBuffer.setDoop(dups);
							allInserted = writeBuffer.setAR(ar);
							if (!allInserted) {
								retries++;
								if (retries > 10) {
									throw new IOException("Unable to write retried " + retries);
								}
								SDFSLogger.getLog().warn("Data was not all inserted, will retry");
							}
						} catch (BufferClosedException e) {
							return;
						} catch (DataArchivedException e) {
							throw e;
						} catch (Exception e) {
							this.errOccured = true;
							throw e;
						} finally {

						}
					}
					/*
					 * if (hashloc[1] == 0 && !Main.chunkStoreLocal) throw new IOException(
					 * "unable to write chunk hash location at 1 = " + hashloc[1]);
					 */
					mf.getIOMonitor().addVirtualBytesWritten(writeBuffer.capacity(), true);
					if (writeBuffer.isNewChunk()) {
						mf.getIOMonitor().addActualBytesWritten(writeBuffer.capacity() - writeBuffer.getDoop(), true);
					} else {
						int prev = (writeBuffer.capacity() - writeBuffer.getPrevDoop());
						int nw = writeBuffer.capacity() - writeBuffer.getDoop();

						mf.getIOMonitor().addActualBytesWritten(nw - prev, true);
					}
					mf.getIOMonitor().addDulicateData((dups - writeBuffer.getPrevDoop()), true);
					this.updateMap(writeBuffer, dups);
				} catch (DataArchivedException e) {
					throw e;
				} catch (Exception e) {
					SDFSLogger.getLog().fatal("unable to add chunk at position " + writeBuffer.getFilePosition(), e);
					this.errOccured = true;
					throw new IOException(e);
				} finally {

				}

			} else if (writeBuffer.isHlAdded()) {

				this.dirty = true;
				this.updateMap(writeBuffer, writeBuffer.getPrevDoop());
			}
		} finally {

		}

	}

	public void updateExtents(WritableCacheBuffer writeBuffer) throws FileClosedException, IOException {
		/*
		 * mf.getIOMonitor().addVirtualBytesWritten(writeBuffer.capacity(), true); if
		 * (writeBuffer.isNewChunk()) {
		 * mf.getIOMonitor().addActualBytesWritten(writeBuffer.capacity() -
		 * writeBuffer.getDoop(), true); } else { int prev = (writeBuffer.capacity() -
		 * writeBuffer.getPrevDoop()); int nw = writeBuffer.capacity() -
		 * writeBuffer.getDoop();
		 *
		 * mf.getIOMonitor().addActualBytesWritten(nw - prev, true); }
		 * mf.getIOMonitor().addDulicateData((writeBuffer.capacity() -
		 * writeBuffer.getPrevDoop()), true);
		 */
		this.updateMap(writeBuffer, writeBuffer.capacity());
	}

	@Override
	public void updateMap(DedupChunkInterface writeBuffer, int doop) throws FileClosedException, IOException {
		this.updateMap(writeBuffer, doop, true);
	}

	// private ReentrantLock updatelock = new ReentrantLock();
	@Override
	public void updateMap(DedupChunkInterface writeBuffer, int doop, boolean propigate)
			throws FileClosedException, IOException {
		if (this.closed) {
			throw new FileClosedException("file already closed");
		}
		SparseDataChunk chunk = null;
		try {
			// updatelock.lock();
			long filePosition = writeBuffer.getFilePosition();
			chunk = new SparseDataChunk(doop, writeBuffer.getFingers(), false, this.bdb.getVersion());
			chunk.setRecontructed(chunk.isRecontructed());
			// SDFSLogger.getLog().info("Hash Size =" + hash.length +
			// " hashloc len = " + writeBuffer.getHashLoc().length);

			bdb.put(filePosition, chunk);
			eventBus.post(new SFileWritten(this, filePosition));
		} catch (Exception e) {
			SDFSLogger.getLog().fatal(
					"unable to write " + writeBuffer.getFilePosition() + " updating map " + mf.getPath(),
					e);
			throw new IOException(e);
		} finally {
			chunk = null;
		}

	}

	public void updateMap(SparseDataChunk chunk, long filePosition) throws FileClosedException, IOException {
		if (this.closed) {
			throw new FileClosedException("file already closed");
		}
		if (filePosition % Main.CHUNK_LENGTH != 0) {
			SDFSLogger.getLog()
					.error("file position requested " + filePosition + " is not divisible by " + Main.CHUNK_LENGTH);
			throw new IOException(
					"file position requested " + filePosition + " is not divisible by " + Main.CHUNK_LENGTH);
		}
		try {

			this.writeBuffers.invalidate(filePosition);
			mf.getIOMonitor().addVirtualBytesWritten(chunk.len, true);
			mf.getIOMonitor().addActualBytesWritten(chunk.len - chunk.getDoop(), true);
			mf.getIOMonitor().addDulicateData(chunk.getDoop(), true);
			chunk.setVersion(this.bdb.getVersion());
			bdb.put(filePosition, chunk);
			this.dirty = true;
			eventBus.post(new SFileWritten(this, filePosition));
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to write " + filePosition + " updating map " + mf.getPath(),
					e);
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

	public SparseDataChunk getSparseDataChunk(long pos) throws IOException, FileClosedException {
		if (pos > mf.length())
			return new SparseDataChunk();
		if (this.closed) {
			throw new FileClosedException("file already closed");
		}
		if (this.toOccured) {
			this.toOccured = false;
			throw new IOException("timeout occured");
		}
		if (this.errOccured) {
			throw new IOException("write error occured");
		}
		if (pck != null && pck.getFpos() == pos)
			return pck;
		else
			pck = bdb.get(pos);
		if (pck == null) {
			pck = new SparseDataChunk();

		}
		pck.setFpos(pos);
		if (pos + Main.CHUNK_LENGTH > mf.length())
			pck.len = (int) (mf.length() - pos);
		else
			pck.len = Main.CHUNK_LENGTH;
		return pck;
	}

	private SparseDataChunk pck = null;

	@Override
	public DedupChunkInterface getWriteBuffer(long position) throws IOException, FileClosedException {
		Lock l = this.globalLock.readLock();
		l.lock();
		try {
			if (this.closed) {
				throw new FileClosedException("file already closed");
			}
			if (!Volume.getStorageConnected())
				throw new IOException("storage offline");
			long chunkPos = this.getChuckPosition(position);
			try {
				WritableCacheBuffer wb = null;
				wb = writeBuffers.get(chunkPos);
				/*
				 * SDFSLogger.getLog() .info("active buffers=" + this.activeBuffers.size() +
				 * " flushingBuffers=" + this.flushingBuffers.size() + " open buffers=" +
				 * this.openBuffers.size() + " writeBuffers=" + writeBuffers.size());
				 */
				return wb;

			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				if (e.getCause() instanceof FileClosedException)
					throw (FileClosedException) e.getCause();
				else
					throw new IOException(e);
			}
		} finally {
			l.unlock();
		}
	}

	private WritableCacheBuffer marshalWriteBuffer(long chunkPos) throws IOException, FileClosedException {
		DedupChunk ck = null;
		try {
			WritableCacheBuffer writeBuffer = null;
			ck = this.getHash(chunkPos, true);

			if (ck.isNewChunk()) {
				writeBuffer = new WritableCacheBuffer(chunkPos, ck.getLength(), this, ck.getFingers(),
						ck.getReconstructed());
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
			SDFSLogger.getLog().warn("Table does not exist in database " + this.GUID, e);
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
	public void sync(boolean force, boolean propigate) throws FileClosedException, IOException {
		this.syncLock.lock();
		try {
			if (!Volume.getStorageConnected())
				throw new IOException("storage offline");
			if (this.closed) {
				return;

			}

			if (Main.safeSync) {
					SDFSLogger.getLog().debug("sync " + mf.getPath());
				//long tm = 0;
				//long wt = 0;
				//long st = 0;
				//tm = System.currentTimeMillis();
				//long wsz = this.writeBuffers.size();
				//int fsz = 0;
				//synchronized (flushingBuffers) {
				//	fsz = this.flushingBuffers.size();
				//}
				this.writeCache();
				//wt = System.currentTimeMillis() - tm;
				HCServiceProxy.sync();
				try {
					this.bdb.sync();
				} catch (Exception e) {

				}
				//st = System.currentTimeMillis() - tm - wt;
				/* 				SDFSLogger.getLog().debug(
						"Sync wb=[" + wsz + "] fb=[" + fsz + "] write fush [" + wt + "] bd sync [" + st + "]");
						*/

			} /*
				 * else { this.writeCache(); }
				 */

			if (this.toOccured) {
				this.toOccured = false;
				throw new IOException("timeout occured");
			}
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			this.syncLock.unlock();
		}
	}

	public void forceRemoteSync() throws IOException {
		DedupFileChannel ch = this.getChannel(-1);
		this.dirty = true;
		eventBus.post(new SFileWritten(this));
		this.unRegisterChannel(ch, -1);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see com.annesam.sdfs.io.AbstractDedupFile#getChannel()
	 */
	@Override
	public DedupFileChannel getChannel(int flags) throws IOException {
		if (!Volume.getStorageConnected())
			throw new IOException("storage offline");
		if (this.toOccured) {
			this.toOccured = false;
			throw new IOException("timeout occured");
		}
		if (!Main.safeClose || Main.blockDev) {
			if (this.staticChannel == null) {
				if (this.isClosed())
					this.initDB();
				this.staticChannel = new DedupFileChannel(this, -1);
			}
			return this.staticChannel;
		} else {
			synchronized (channels) {
				if (this.isClosed() || this.channels.size() == 0)
					this.initDB();
				DedupFileChannel channel = new DedupFileChannel(this, flags);

				this.channels.add(channel);
				return channel;
			}

		}

	}

	/*
	 * (non-Javadoc)
	 *
	 * @see com.annesam.sdfs.io.AbstractDedupFile#unRegisterChannel(com.annesam.sdfs
	 * .io.DedupFileChannel)
	 */
	@Override
	public void unRegisterChannel(DedupFileChannel channel, int flags) {
		if (Main.safeClose && !Main.blockDev) {
			synchronized (channels) {
				try {

					// if (channel.getFlags() == flags) {
					this.channels.remove(channel);
					channel.close(flags);
					SDFSLogger.getLog().debug("Channel size is " + this.channels.size());
					if (this.channels.size() == 0) {
						SDFSLogger.getLog().debug("Closinging " + this.mf.getPath());
						this.forceClose();
					}
					// } else {
					// SDFSLogger.getLog().warn("unregister of filechannel for [" +
					// this.mf.getPath()
					// + "] failed because flags mismatch flags [" + flags + "!=" +
					// channel.getFlags() + "]");
					// }
					try {
						MetaFileStore.getMF(mf.getPath()).sync();
						eventBus.post(new SFileWritten(this));
					} catch (Exception e) {
						SDFSLogger.getLog().error("error while syncing file in close", e);
					}

				} catch (Exception e) {
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see com.annesam.sdfs.io.AbstractDedupFile#unRegisterChannel(com.annesam.sdfs
	 * .io.DedupFileChannel)
	 */
	@Override
	public void registerChannel(DedupFileChannel channel) throws IOException {
		if (!Volume.getStorageConnected())
			throw new IOException("storage offline");
		if (this.toOccured) {
			this.toOccured = false;
			throw new IOException("timeout occured");
		}
		if (!Main.safeClose) {
			try {
				if (this.staticChannel == null) {
					if (this.isClosed())
						this.initDB();
					this.staticChannel = channel;
				}
			} catch (Exception e) {
			}
		} else {
			synchronized (channels) {
				if (this.isClosed() || this.channels.size() == 0)
					this.initDB();
				this.channels.add(channel);
			}
		}
	}

	@Override
	public boolean hasOpenChannels() {
		synchronized (channels) {
			try {
				if (this.channels.size() > 0)
					return true;
				else
					return false;
			} catch (Exception e) {
				return false;
			}
		}
	}

	public int openChannelsSize() {
		synchronized (channels) {
			try {
				return this.channels.size();
			} catch (Exception e) {
				return -1;
			}
		}
	}

	public void closeBuffer(WritableCacheBuffer bf) {
		this.writeBuffers.invalidate(bf.getFilePosition());
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see com.annesam.sdfs.io.AbstractDedupFile#close()
	 */
	@Override
	public void forceClose() throws IOException {
		this.syncLock.lock();
		Lock l = this.globalLock.writeLock();
		l.lock();
		try {
			if (!this.closed) {
				SDFSLogger.getLog().debug("Closing dedupfile [" + mf.getPath() + "] guid=" + this.GUID + " lenght="
						+ MetaFileStore.getMF(mf.getPath()).getPath());
				if (Main.safeClose) {
					try {
						ArrayList<DedupFileChannel> al = new ArrayList<DedupFileChannel>();

						for (DedupFileChannel f : this.channels) {
							al.add(f);
						}
						for (int i = 0; i < al.size(); i++) {
							al.get(i).close(al.get(i).getFlags());
						}
					} catch (Exception e) {
						SDFSLogger.getLog().error("error closing " + mf.getPath(), e);
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
							SDFSLogger.getLog().debug("Flushing " + nwb + " buffers");
					}
						SDFSLogger.getLog().debug("Flushed " + twb + " buffers");

				} catch (Exception e) {
					SDFSLogger.getLog().error("unable to flush " + this.databasePath, e);
				}
				try {
					HashBlobArchive.sync(this.GUID);
				} catch (Exception e) {
					SDFSLogger.getLog().error("unable to sync " + this.GUID, e);
				}
				if (!this.deleted) {

					try {
						this.bdb.sync();
					} catch (Exception e) {
					}
					try {

						MetaFileStore.getMF(mf.getPath()).sync();
						eventBus.post(new SFileWritten(this));
						eventBus.post(new SFileClosed(this));
					} catch (Exception e) {
						SDFSLogger.getLog().error("error while syncing file in close", e);
					}

					try {
						this.bdb.close();
					} catch (Exception e) {
					}
					this.bdb = null;
					this.closed = true;

				} else {
					try {
						this.bdb.close();
					} catch (Exception e) {
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
			if (!Volume.getStorageConnected())
				throw new IOException("storage offline");
			SDFSLogger.getLog().debug("Closed [" + mf.getPath() + "]");
		} finally {
			try {
				DedupFileStore.removeOpenDedupFile(this.GUID);
				bdb = null;
				this.closed = true;
				this.dirty = false;
			} catch (Exception e) {
			}
			try {
				l.unlock();
			} catch (Exception e) {
			}
			try {
				this.syncLock.unlock();
			} catch (Exception e) {
			}
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
	 * @see com.annesam.sdfs.io.AbstractDedupFile#removeLock(com.annesam.sdfs.io.
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
	public DedupFileLock addLock(DedupFileChannel ch, long position, long len, boolean shared) throws IOException {
		return this.addLock(ch, position, len, shared, true);
	}

	@Override
	public DedupFileLock addLock(DedupFileChannel ch, long position, long len, boolean shared, boolean propigate)
			throws IOException {
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
			if (!Volume.getStorageConnected())
				throw new IOException("storage offline");
			if (this.isClosed()) {
				File directory = new File(
						Main.dedupDBStore + File.separator + this.GUID.substring(0, 2) + File.separator + this.GUID);
				File dbf = new File(directory.getPath() + File.separator + this.GUID + ".map");
				this.databaseDirPath = directory.getPath();
				this.databasePath = dbf.getPath();
				if (!directory.exists()) {
					directory.mkdirs();
				}

				this.bdb = LongByteArrayMap.getMap(GUID);

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
	public DedupChunk getHash(long location, boolean create) throws IOException, FileClosedException {
		if (this.closed) {
			throw new FileClosedException("file already closed");
		}
		long place = this.getChuckPosition(location);
		DedupChunk ck = null;
		try {
			// this.addReadAhead(place);
			SparseDataChunk pck = this.bdb.get(place);
			if (pck != null) {
				// ByteString data = pck.getData();
				// boolean dataEmpty = !pck.isLocalData();
				// if (dataEmpty) {
				ck = new DedupChunk(place, Main.CHUNK_LENGTH, false, pck.getFingers(), pck.isRecontructed());
				// } else {
				// byte dk[] = chunkStore.get(place);
				// ck = new DedupChunk(dk, place,
				// Main.CHUNK_LENGTH, pck.getHashLoc(), pck.getLen(),
				// pck.getPos());
				// }
				ck.setDoop(pck.getDoop());
				pck = null;
			}
			if (ck == null && create == true) {
				return createNewChunk(place);
			} else {
				return ck;
			}
		} catch (FileClosedException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to fetch chunk at position " + place, e);

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
			SDFSLogger.getLog().warn("unable to remove chunk at position " + place, e);
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
		DedupChunk ck = new DedupChunk(location, Main.CHUNK_LENGTH, true, new TreeMap<Integer, HashLocPair>(), false);
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
	public void putBufferIntoFlush(WritableCacheBuffer writeBuffer) {
		this.flushingBuffers.put(writeBuffer.getFilePosition(), writeBuffer);
	}

	@Override
	public void removeBufferFromFlush(WritableCacheBuffer writeBuffer) {
		this.flushingBuffers.remove(writeBuffer.getFilePosition());
	}

	public void addOpenBuffer(WritableCacheBuffer bf) {
		this.openBuffers.put(bf.getFilePosition(), bf);
		// SDFSLogger.getLog().info("open Size is " +this.openBuffers.size());
	}

	public void removeOpenBuffer(WritableCacheBuffer bf) {
		this.openBuffers.remove(bf.getFilePosition());
		// SDFSLogger.getLog().info("Size is " +this.openBuffers.size());
	}

	public boolean bufferInFlush(WritableCacheBuffer writeBuffer) {
		return this.flushingBuffers.containsKey(writeBuffer.getFilePosition());
	}

	@Override
	public void trim(long start, int len) throws IOException {
		try {
			this.bdb.trim(start, len);
		} catch (FileClosedException e) {
			throw new IOException(e);
		}
	}

	ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock();

	public Lock getReadLock() {
		return globalLock.readLock();
	}

	public Lock getWriteLock() {
		return globalLock.writeLock();
	}

}