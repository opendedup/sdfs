package org.opendedup.sdfs.filestore;

import java.io.File;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantLock;

import org.bouncycastle.util.Arrays;
import org.opendedup.hashing.AbstractHashEngine;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.util.EncryptUtils;
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
	private static final int pageSize = Main.chunkStorePageSize;
	private boolean closed = false;
	private FileChannel fc = null;
	private RandomAccessFile chunkDataWriter = null;
	private static File chunk_location = new File(Main.chunkStore);
	File f;
	Path p;
	private long currentLength = 0L;
	private String name;

	private byte[] FREE = new byte[pageSize];
	private FileChannel iterFC = null;
	private AbstractHashEngine hc = null;
	private SyncThread th = null;

	/**
	 * 
	 * @param name
	 *            the name of the chunk store.
	 */
	public FileChunkStore() {
		SDFSLogger.getLog().info("Opening Chunk Store");
		Arrays.fill(FREE, (byte) 0);
		try {
			if (!chunk_location.exists()) {
				chunk_location.mkdirs();
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
			SDFSLogger.getLog().info("ChunkStore " + f.getPath() + " created");
			th = new SyncThread(this);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public FileChunkStore(String fpath) {
		SDFSLogger.getLog().info("Opening Chunk Store");
		Arrays.fill(FREE, (byte) 0);
		try {
			if (!chunk_location.exists()) {
				chunk_location.mkdirs();
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

	private static ReentrantLock reservePositionlock = new ReentrantLock();

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.annesam.sdfs.filestore.AbstractChunkStore#reserveWritePosition(int)
	 */
	@Override
	public long reserveWritePosition(int len) throws IOException {
		if (this.closed)
			throw new IOException("ChunkStore is closed");
		reservePositionlock.lock();
		long pos = this.currentLength;
		this.currentLength = this.currentLength + pageSize;
		reservePositionlock.unlock();
		return pos;
	}

	@Override
	public void claimChunk(byte[] hash, long pos) throws IOException {
		/*
		 * RandomAccessFile raf = new RandomAccessFile(this.meta_location,
		 * "rw"); FileChannel ch = raf.getChannel(); raf.seek((pos / pageSize) *
		 * metaPageSize); byte [] raw = new byte[ChunkMetaData.RAWDL];
		 * raf.read(raw); ChunkMetaData cm = new ChunkMetaData(raw);
		 * cm.setmDelete(false); cm.setLastClaimed(System.currentTimeMillis());
		 * ch.position((pos / pageSize) * metaPageSize);
		 * ch.write(cm.getBytes()); ch.close(); raf.close(); ch = null; raf =
		 * null; raw = null; cm = null;
		 */
	}

	@Override
	public void writeChunk(byte[] hash, byte[] chunk, int len, long start)
			throws IOException {
		if (this.closed)
			throw new IOException("ChunkStore is closed");
		ByteBuffer buf = null;
		try {
			if (Main.chunkStoreEncryptionEnabled)
				chunk = EncryptUtils.encrypt(chunk);
			buf = ByteBuffer.wrap(chunk);
			fc.write(buf, start);

		} catch (Exception e) {
			SDFSLogger.getLog().fatal(
					"unable to write data at position " + start, e);
			throw new IOException("unable to write data at position " + start);
		} finally {
			buf = null;
			hash = null;
			chunk = null;
			len = 0;
			start = 0;
		}
	}

	@Override
	public byte[] getChunk(byte[] hash, long start, int len) throws IOException {
		if (this.closed)
			throw new IOException("ChunkStore is closed");
		// long time = System.currentTimeMillis();

		ByteBuffer fbuf = ByteBuffer.wrap(new byte[pageSize]);

		try {
			fc.read(fbuf, start);
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to fetch chunk at position " + start, e);
			throw new IOException(e);
		} finally {
			try {
			} catch (Exception e) {
			}
		}
		return fbuf.array();
	}

	@Override
	public void deleteChunk(byte[] hash, long start, int len)
			throws IOException {
		if (this.closed)
			throw new IOException("ChunkStore is closed");
		RandomAccessFile raf = new RandomAccessFile(f, "rw");
		raf.seek(start);
		raf.write(0);
		raf.close();
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
	public boolean moveChunk(byte[] hash, long origLoc, long newLoc)
			throws IOException {
		if (this.closed)
			throw new IOException("ChunkStore is closed");
		byte[] buf = new byte[Main.chunkStorePageSize];
		RandomAccessFile raf = new RandomAccessFile(f, "rw");
		try {
			raf.seek(origLoc);
			raf.read(buf);
			if (Main.preAllocateChunkStore && Arrays.areEqual(FREE, buf))
				return false;
			raf.seek(newLoc);
			raf.write(buf);
			if (!Main.preAllocateChunkStore
					&& (origLoc + Main.chunkStorePageSize) == raf.length())
				raf.setLength(origLoc);
			return true;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal(
					"could not move data from [" + origLoc + "] to [" + newLoc
							+ "]", e);
			return false;
		} finally {
			raf.close();
			raf = null;
			buf = null;
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

	@Override
	public void compact() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void addChunkStoreListener(AbstractChunkStoreListener listener) {
		// TODO Auto-generated method stub

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
