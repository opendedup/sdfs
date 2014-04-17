package org.opendedup.sdfs.filestore;

import java.io.File;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantLock;

//import org.apache.lucene.store.NativePosixUtil;
import org.apache.lucene.util.OpenBitSet;
import org.bouncycastle.util.Arrays;
import org.opendedup.hashing.AbstractHashEngine;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.util.OpenBitSetSerialize;
import org.w3c.dom.Element;

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
	//private FileChannel fc = null;
	private RandomAccessFile chunkDataWriter = null;
	File f;
	Path p;
	private long currentLength = 0;
	private String name;
	private OpenBitSet freeSlots = null;
	private byte[] FREE = new byte[pageSize];
	private FileChannel iterFC = null;
	private AbstractHashEngine hc = null;
	private SyncThread th = null;
	private File bsf;
	private FCPool pool = null;

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
				if(SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("Looks like a new ChunkStore");
				/*
				 * this.freeSlots = new OpenBitSet(
				 * (Main.chunkStoreAllocationSize / Main.chunkStorePageSize));
				 */
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
			pool = new FCPool(f, 100);
			SDFSLogger.getLog().info("ChunkStore " + f.getPath() + " created");
			th = new SyncThread(this);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to open filestore", e);
			e.printStackTrace();
			System.exit(-1);
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
				if(SDFSLogger.isDebug())
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
			pool = new FCPool(f, 100);
			SDFSLogger.getLog().info("ChunkStore " + f.getPath() + " created");
			th = new SyncThread(this);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to open filestore" + fpath, e);
			e.printStackTrace();
			System.exit(-1);
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
			//NativePosixUtil.advise(chunkDataWriter.getFD(), 0, 0, NativePosixUtil.SEQUENTIAL);
			this.currentLength=chunkDataWriter.length();
			this.closed = false;
			pool = new FCPool(f, 100);
			SDFSLogger.getLog().info("ChunkStore " + f.getPath() + " created");
			th = new SyncThread(this);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to open filestore" + fpath, e);
			e.printStackTrace();
			System.exit(-1);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.filestore.AbstractChunkStore#closeStore()
	 */
	public void closeStore() {
		SDFSLogger.getLog().info("Closing chunkstore " +this.name);
		try {
			this.chunkDataWriter.getFD().sync();

		} catch (Exception e) {
		}

		try {
			this.chunkDataWriter.close();

		} catch (Exception e) {
		}
		try {
			this.pool.close();
		} catch (Exception e) {

		}
		try {
			if (this.freeSlots != null) {
				OpenBitSetSerialize.writeOut(bsf.getPath(), this.freeSlots);
				SDFSLogger.getLog().info("Persisted Free Slots");
				this.freeSlots.clear(0, this.freeSlots.capacity());
			}
		} catch (Exception e) {
		}
	}

	protected void sync() throws IOException {
		//this.chunkDataWriter.getFD().sync();
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
		if (this.freeSlots != null) {
		return this.freeSlots.cardinality();
		}
		else 
			return 0;
	}

	private ReentrantLock rlock = new ReentrantLock();

	long smallestFree = 0;
	public long writeChunk(byte[] hash, byte[] chunk, int len)
			throws IOException {
		if (this.closed)
			throw new IOException("ChunkStore is closed");
		long pos = -1;
		FileChannel rf = null;
		try {
			rlock.lock();
			if (this.freeSlots != null) {
				try {
					pos = this.freeSlots.nextSetBit(smallestFree);
					if (pos >= 0) {
						this.smallestFree = pos;
						this.freeSlots.fastClear(pos);
					} else {
						this.smallestFree = 0;
						this.freeSlots = null;
					}
				} catch (Throwable e) {
					SDFSLogger.getLog().warn(e);
				}
				if (pos >= 0) {
					pos = pos * this.pageSize;
				}
			}
			if (pos < 0) {
				pos = this.currentLength;
				this.currentLength = this.currentLength + pageSize;
			}
			rlock.unlock();
			
			// this.chunks.invalidate(Long.valueOf(pos));
			rf = pool.borrowObject();
			ByteBuffer buf = ByteBuffer.wrap(new byte [pageSize]);
			buf.put(chunk);
			buf.position(0);
			rf.write(buf, pos);
			
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
		if(len > pageSize)
			throw new IOException("length is greater than page size");
		if(len == -1) 
			len = pageSize;
		byte[] b = new byte[len];
		FileChannel rf = pool.borrowObject();
		try {
			rf.read(ByteBuffer.wrap(b), start);
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to fetch chunk at position " + start, e);
			throw new IOException(e);
		} finally {
			try {
				pool.returnObject(rf);
			} catch (Exception e) {
			}
		}
		return b;
	}

	@Override
	public void deleteChunk(byte[] hash, long start, int len)
			throws IOException {
		if (this.closed)
			throw new IOException("ChunkStore is closed");
		long pos = start / this.pageSize;
		this.rlock.lock();
		if (this.freeSlots == null) {
			this.freeSlots = new OpenBitSet();
			this.smallestFree = 0;
		}
		if(this.smallestFree > pos)
			this.smallestFree = pos;
		this.freeSlots.ensureCapacity(pos);
		this.freeSlots.set(pos);
		this.rlock.unlock();
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
			raf.close();
		} catch (Exception e) {
			SDFSLogger.getLog().warn("while closing filechunkstore ", e);
		}
	}

	@Override
	public void init(Element config) {
		// TODO Auto-generated method stub

	}

	@Override
	public ChunkData getNextChunck() throws IOException {
		if (iterFC.position() >= iterFC.size()) {
			this.iterFC = null;
			return null;
		}
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
		chk.cLen = fbuf.array().length;
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
			th = new Thread(this);
			th.start();
		}

		@Override
		public void run() {
			while (!store.closed) {
				try {
					Thread.sleep(interval);
					store.sync();
				} catch (IOException e) {
					if(SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("Unable to flush FileChunkStore ",
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

	@Override
	public long maxSize() {
		return Main.chunkStoreAllocationSize;
	}

	@Override
	public long compressedSize() {
		return this.currentLength;
	}

	@Override
	public void deleteDuplicate(byte[] hash, long start, int len)
			throws IOException {
		this.deleteChunk(hash, start, len);
		
	}

}
