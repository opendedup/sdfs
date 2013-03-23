package org.opendedup.sdfs.filestore;

import java.io.File;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.util.OpenBitSet;
import org.bouncycastle.util.Arrays;
import org.opendedup.hashing.AbstractHashEngine;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.util.OpenBitSetSerialize;
import org.w3c.dom.Element;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * 
 * @author Sam Silverberg This chunk store saves chunks into a single contiguous
 *         file. It performs well on most file systems. This chunkstore is used
 *         by default. The chunkstore is called by the hashstore to save or
 *         retrieve deduped chunks. Chunk block meta data is as follows: [mark
 *         of deletion (1 byte)|hash lenth(2 bytes)|hash(32 bytes)|date added (8
 *         bytes)|date last accessed (8 bytes)| chunk len (4 bytes)|chunk
 *         position (8 bytes)]
 **/
public class FileChunkStore implements AbstractChunkStore {
	private int pageSize = Main.chunkStorePageSize;
	private boolean closed = false;
	private FileChannel fc = null;
	private RandomAccessFile chunkDataWriter = null;
	File f;
	Path p;
	private long currentLength = 0L;
	private String name;
	private OpenBitSet freeSlots = null;
	private byte[] FREE = new byte[pageSize];
	private FileChannel iterFC = null;
	private AbstractHashEngine hc = null;
	private SyncThread th = null;
	private File bsf;
	private RAFPool pool = null;
	private int cacheSize = 104857600 / Main.CHUNK_LENGTH;
	
	LoadingCache<Long, byte []> chunks = CacheBuilder.newBuilder()
			.maximumSize(cacheSize).concurrencyLevel(72)
			.build(new CacheLoader<Long, byte []>() {
				public byte [] load(Long key) throws IOException {
					byte[] b = new byte[pageSize];
					
					RandomAccessFile rf = pool.borrowObject();
					try {
						rf.seek(key);
						rf.read(b);
					} catch (Exception e) {
						SDFSLogger.getLog().error(
								"unable to fetch chunk at position " + key, e);
						throw new IOException(e);
					} finally {
						try {
							pool.returnObject(rf);
						} catch (Exception e) {
						}
					}
					
						
					return b;
				}
			});

	/**
	 * 
	 * @param name
	 *            the name of the chunk store.
	 */
	public FileChunkStore() {
		SDFSLogger.getLog().info("Opening Chunk Store");
		Arrays.fill(FREE, (byte) 0);
		try {
			File chunk_location = new File(Main.chunkStore);
			if (!chunk_location.exists()) {
				chunk_location.mkdirs();
			}
			SDFSLogger.getLog().info("Loading freebits bitset");
			bsf = new File(chunk_location + File.separator + "freebit.map");
			if (!bsf.exists()) {
				SDFSLogger.getLog().debug("Looks like a new ChunkStore");
				this.freeSlots = new OpenBitSet();
			} else {
				SDFSLogger.getLog().info(
						"Loading freeslots from " + bsf.getPath());
				try {
					this.freeSlots = OpenBitSetSerialize.readIn(bsf.getPath());
					bsf.delete();
				} catch (Exception e) {
					SDFSLogger.getLog().error(
							"Unable to load bitset from " + bsf.getPath(), e);
				}
				SDFSLogger.getLog().info(
						"Loaded [" + this.freeSlots.cardinality()
								+ "] free slots");
			}
			f = new File(chunk_location + File.separator + "chunks.chk");
			if (!f.getParentFile().exists())
				f.getParentFile().mkdirs();
			this.name = "chunks";
			p = f.toPath();
			chunkDataWriter = new RandomAccessFile(f, "rw");
			this.currentLength = chunkDataWriter.length();
			this.closed = false;
			fc = chunkDataWriter.getChannel();
			pool = new RAFPool(f, 10);
			SDFSLogger.getLog().info("ChunkStore " + f.getPath() + " created");
			th = new SyncThread(this);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public FileChunkStore(String fpath) {
		SDFSLogger.getLog().info("Opening Chunk Store " + fpath);
		File pf = new File(fpath).getParentFile();
		Arrays.fill(FREE, (byte) 0);
		try {
			SDFSLogger.getLog().info("Loading freebits bitset");
			bsf = new File(pf.getPath() + File.separator + "freebit.map");
			if (!bsf.exists()) {
				SDFSLogger.getLog().debug("Looks like a new ChunkStore");
				this.freeSlots = new OpenBitSet();
			} else {
				SDFSLogger.getLog().info(
						"Loading freeslots from " + bsf.getPath());
				try {
					this.freeSlots = OpenBitSetSerialize.readIn(bsf.getPath());
					bsf.delete();
				} catch (Exception e) {
					SDFSLogger.getLog().error(
							"Unable to load bitset from " + bsf.getPath(), e);
				}
				SDFSLogger.getLog().info(
						"Loaded [" + this.freeSlots.cardinality()
								+ "] free slots");
			}
			f = new File(fpath);
			if (!f.getParentFile().exists())
				f.getParentFile().mkdirs();
			this.name = "chunks";
			p = f.toPath();
			chunkDataWriter = new RandomAccessFile(f, "rw");
			this.currentLength = chunkDataWriter.length();
			this.closed = false;
			fc = chunkDataWriter.getChannel();
			pool = new RAFPool(f, 10);
			SDFSLogger.getLog().info("ChunkStore " + f.getPath() + " created");
			th = new SyncThread(this);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public FileChunkStore(String fpath, int chunkLength) {
		SDFSLogger.getLog().info(
				"Opening Chunk Store [" + fpath + "] with chunksize of "
						+ chunkLength);
		this.pageSize = chunkLength;
		FREE = new byte[pageSize];
		Arrays.fill(FREE, (byte) 0);
		try {
			SDFSLogger.getLog().info("Loading freebits bitset");
			bsf = new File(fpath + "freebit.map");
			if (!bsf.exists()) {
				SDFSLogger.getLog().debug("Looks like a new ChunkStore");
				this.freeSlots = new OpenBitSet();
			} else {
				SDFSLogger.getLog().info(
						"Loading freeslots from " + bsf.getPath());
				try {
					this.freeSlots = OpenBitSetSerialize.readIn(bsf.getPath());
					bsf.delete();
				} catch (Exception e) {
					SDFSLogger.getLog().error(
							"Unable to load bitset from " + bsf.getPath(), e);
				}
				SDFSLogger.getLog().info(
						"Loaded [" + this.freeSlots.cardinality()
								+ "] free slots");
			}
			f = new File(fpath + ".chk");
			if (!f.getParentFile().exists())
				f.getParentFile().mkdirs();
			this.name = "chunks";
			p = f.toPath();
			chunkDataWriter = new RandomAccessFile(f, "rw");
			this.currentLength = chunkDataWriter.length();
			this.closed = false;
			fc = chunkDataWriter.getChannel();
			pool = new RAFPool(f, 10);
			SDFSLogger.getLog().info("ChunkStore " + f.getPath() + " created");
			th = new SyncThread(this);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.filestore.AbstractChunkStore#closeStore()
	 */
	public void closeStore() {

		try {
			fc.force(true);

		} catch (Exception e) {
		}

		try {
			fc.close();

		} catch (Exception e) {
		}
		fc = null;
		try {
			OpenBitSetSerialize.writeOut(bsf.getPath(), this.freeSlots);
			SDFSLogger.getLog().info("Persisted Free Slots");
			this.freeSlots.clear(0, this.freeSlots.capacity());
		} catch (Exception e) {
		}
	}

	protected void sync() throws IOException {
		fc.force(true);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.filestore.AbstractChunkStore#getName()
	 */
	@Override
	public String getName() {
		return name;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.annesam.sdfs.filestore.AbstractChunkStore#setName(java.lang.String)
	 */
	@Override
	public void setName(String name) {
		this.name = name;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.filestore.AbstractChunkStore#size()
	 */
	@Override
	public long size() {
		return this.currentLength;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.filestore.AbstractChunkStore#bytesRead()
	 */
	@Override
	public long bytesRead() {
		return this.bytesRead();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.filestore.AbstractChunkStore#bytesWritten()
	 */
	@Override
	public long bytesWritten() {
		return this.bytesWritten();
	}

	@Override
	public long getFreeBlocks() {
		return this.freeSlots.cardinality();
	}

	private static ReentrantLock reservePositionlock = new ReentrantLock();

	@Override
	public long writeChunk(byte[] hash, byte[] chunk, int len)
			throws IOException {
		if (this.closed)
			throw new IOException("ChunkStore is closed");
		long pos = -1;
		RandomAccessFile rf = null;
		try {
			reservePositionlock.lock();
			pos = this.freeSlots.nextSetBit(0);
			if (pos < 0) {
				pos = this.currentLength;
				this.currentLength = this.currentLength + pageSize;
			} else {
				this.freeSlots.clear(pos);
				pos = pos * this.pageSize;
			}
			reservePositionlock.unlock();
			this.chunks.invalidate(Long.valueOf(pos));
			rf = pool.borrowObject();
			rf.seek(pos);
			rf.write(chunk);
			return pos;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal(
					"unable to write data at position " + pos, e);
			throw new IOException("unable to write data at position " + pos);
		} finally {
			try {
				pool.returnObject(rf);
			} catch (Exception e) {
			}
			hash = null;
			chunk = null;
			len = 0;
		}
	}

	@Override
	public byte[] getChunk(byte[] hash, long start, int len) throws IOException {
		if (this.closed)
			throw new IOException("ChunkStore is closed");
		try {
			byte[] _bz = this.chunks.get(Long.valueOf(start));
			byte[] bz = Arrays.clone(_bz);
			return bz;
		} catch (ExecutionException e) {
			SDFSLogger.getLog().error("Unable to get block at " + start, e);
			throw new IOException(e);
		} 
	}

	@Override
	public void deleteChunk(byte[] hash, long start, int len)
			throws IOException {
		if (this.closed)
			throw new IOException("ChunkStore is closed");
		this.freeSlots.set(start / this.pageSize);
		this.chunks.invalidate(Long.valueOf(start));
		/*
		 * RandomAccessFile raf = new RandomAccessFile(f, "rw");
		 * raf.seek(start); raf.write(0); raf.close();
		 */
	}

	@Override
	public void close() {
		th.close();
		try {
			this.closed = true;
			this.closeStore();

			RandomAccessFile raf = new RandomAccessFile(f, "rw");
			raf.getChannel().force(true);
		} catch (Exception e) {
			SDFSLogger.getLog().warn("while closing filechunkstore ", e);
		}
	}

	@Override
	public void init(Element config) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setSize(long size) {
		// TODO Auto-generated method stub

	}

	@Override
	public ChunkData getNextChunck() throws IOException {
		if (iterFC.position() >= iterFC.size())
			return null;
		ByteBuffer fbuf = ByteBuffer.wrap(new byte[pageSize]);
		long pos = -1;
		try {
			pos = iterFC.position();
			iterFC.read(fbuf);
		} catch (Exception e) {
			SDFSLogger.getLog()
					.error("unable to fetch chunk at position "
							+ iterFC.position(), e);
			throw new IOException(e);
		} finally {
			try {
			} catch (Exception e) {
			}
		}
		byte[] hash = hc.getHash(fbuf.array());
		ChunkData chk = new ChunkData(hash, pos);
		chk.setChunk(fbuf.array());
		return chk;

	}

	private ReentrantLock iterlock = new ReentrantLock();

	@Override
	public void iterationInit() throws IOException {
		this.iterlock.lock();
		try {
			hc = HashFunctionPool.getHashEngine();
			this.iterFC = chunkDataWriter.getChannel();
			this.iterFC.position(0);
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			this.iterlock.unlock();
		}
	}

	private class SyncThread implements Runnable {
		FileChunkStore store = null;
		int interval = 2 * 1000;
		Thread th = null;

		SyncThread(FileChunkStore store) {
			this.store = store;
			Thread th = new Thread(this);
			th.start();
		}

		@Override
		public void run() {
			while (!store.closed) {
				try {
					store.sync();
					Thread.sleep(interval);
				} catch (IOException e) {
					SDFSLogger.getLog().warn("Unable to flush FileChunkStore ",
							e);
				} catch (InterruptedException e) {
					break;
				}
			}

		}

		public void close() {
			try {
				th.interrupt();
			} catch (Exception e) {
			}

		}
	}

}
