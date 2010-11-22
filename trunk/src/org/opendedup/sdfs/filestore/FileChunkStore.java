package org.opendedup.sdfs.filestore;

import java.io.File;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;

import org.bouncycastle.util.Arrays;
import org.opendedup.sdfs.Main;
import org.opendedup.util.EncryptUtils;
import org.opendedup.util.SDFSLogger;

/**
 * 
 * @author Sam Silverberg This chunk store saves chunks into a single contiguous
 *         file. It performs well on most file systems. This chunkstore is used
 *         by default. The chunkstore is called by the hashstore to save or
 *         retrieve deduped chunks. Chunk block meta data is as follows: [mark
 *         of deletion (1 byte)|hash lenth(2 bytes)|hash(32 bytes)|date added (8
 *         bytes)|date last accessed (8 bytes)| chunk len (4 bytes)|chunk
 *         position (8 bytes)]
 */
public class FileChunkStore implements AbstractChunkStore {
	private static ArrayList<FileChunkStore> stores = new ArrayList<FileChunkStore>();
	private static final int pageSize = Main.chunkStorePageSize;
	private String name;
	private boolean closed = false;
	private FileChannel chunkDataReader = null;
	private RandomAccessFile chunkDataWriter = null;
	private RandomAccessFile posRaf = null;
	private RandomAccessFile in = null;
	private static File chunk_location = new File(Main.chunkStore);
	File f;
	Path p;
	private long currentLength = 0L;
	private ArrayList<AbstractChunkStoreListener> listeners = new ArrayList<AbstractChunkStoreListener>();
	/*
	 * private static ConcurrentLinkedHashMap<String, ByteBuffer> cache = new
	 * Builder<String,ByteBuffer>().maximumWeightedCapacity(MAX_ENTRIES)
	 * .concurrencyLevel(Main.writeThreads).build();
	 */
	private byte[] FREE = new byte[pageSize];

	/**
	 * 
	 * @param name
	 *            the name of the chunk store.
	 */
	public FileChunkStore(String name) {
		SDFSLogger.getLog().info("Opening Chunk Store");
		Arrays.fill(FREE, (byte) 0);
		try {
			if (!chunk_location.exists()) {
				chunk_location.mkdirs();
			}
			this.name = name;
			f = new File("/dev/sda1");
			p = f.toPath();
			File posFile = new File(chunk_location + File.separator + name
					+ ".pos");
			boolean newPos = true;
			if (posFile.exists())
				newPos = false;
			else {
				posFile.getParentFile().mkdirs();
			}
			posRaf = new RandomAccessFile(posFile, "rw");
			if (!newPos) {
				posRaf.seek(0);
				this.currentLength = posRaf.readLong();
			} else {
				posRaf.seek(0);
				posRaf.writeLong(currentLength);
			}
			chunkDataWriter = new RandomAccessFile(f, "rw");
			in = new RandomAccessFile(f, "r");
			chunkDataReader = in.getChannel();
			if (Main.preAllocateChunkStore)
				this.expandFile(Main.chunkStoreAllocationSize);
			this.closed = false;
			stores.add(this);
			SDFSLogger.getLog().info("ChunkStore " + this.name + " created");
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
			synchronized (chunkDataWriter) {
				chunkDataWriter.close();
			}
		} catch (IOException e) {
		}
		try {
			synchronized (chunkDataReader) {
				chunkDataReader.force(true);
				chunkDataReader.close();
			}
		} catch (IOException e) {
		}
		try {
			synchronized (in) {
				in.close();
			}
		} catch (IOException e) {
		}
		try {
			synchronized (posRaf) {
				posRaf.close();
			}
		} catch (Exception e) {

		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.filestore.AbstractChunkStore#getName()
	 */
	public String getName() {
		return name;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.annesam.sdfs.filestore.AbstractChunkStore#setName(java.lang.String)
	 */
	public void setName(String name) {
		this.name = name;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.filestore.AbstractChunkStore#size()
	 */
	public long size() {
		return this.currentLength;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.filestore.AbstractChunkStore#bytesRead()
	 */
	public long bytesRead() {
		return this.bytesRead();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.filestore.AbstractChunkStore#bytesWritten()
	 */
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
	public long reserveWritePosition(int len) throws IOException {
		if (this.closed)
			throw new IOException("ChunkStore is closed");
		reservePositionlock.lock();
		long pos = this.currentLength;
		this.currentLength = this.currentLength + pageSize;
		this.posRaf.seek(0);
		this.posRaf.writeLong(this.currentLength);
		reservePositionlock.unlock();
		return pos;
	}

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

	// private ReentrantLock furthestPositionlock = new ReentrantLock();

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.filestore.AbstractChunkStore#writeChunk(byte[],
	 * int, long)
	 */
	public void writeChunk(byte[] hash, byte[] chunk, int len, long start)
			throws IOException {
		if (this.closed)
			throw new IOException("ChunkStore is closed");
		RandomAccessFile raf = null;
		try {
			if (Main.chunkStoreEncryptionEnabled)
				chunk = EncryptUtils.encrypt(chunk);
			raf = new RandomAccessFile(f, "rw");
			raf.seek(start);
			raf.write(chunk);

		} catch (Exception e) {
			SDFSLogger.getLog().fatal(
					"unable to write data at position " + start, e);
			throw new IOException("unable to write data at position " + start);
		} finally {

			try {
				raf.close();
			} catch (Exception e) {
			}
			raf = null;
			hash = null;
			chunk = null;
			len = 0;
			start = 0;
		}
	}

	// private ReentrantLock getChunklock = new ReentrantLock();
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.filestore.AbstractChunkStore#getChunk(long, int)
	 */
	
	public byte[] getChunk(byte[] hash, long start, int len) throws IOException {
		if (this.closed)
			throw new IOException("ChunkStore is closed");
		// long time = System.currentTimeMillis();
		RandomAccessFile raf = null;
		byte[] fbuf = new byte[pageSize];
		try {
			raf = new RandomAccessFile(f, "r");
			raf.seek(start);
			raf.read(fbuf);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to fetch chunk at position " + start,e);
			throw new IOException(e);
		} finally {
			try {
				raf.close();
			} catch (Exception e) {
			}
			raf = null;
		}
		return fbuf;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.filestore.AbstractChunkStore#expandFile(long)
	 */
	public synchronized void expandFile(long length) throws IOException {
		if (this.chunkDataWriter.length() < length) {
			SDFSLogger.getLog().info(
					"########### Pre-Allocating Chunkstore to size " + length
							+ " ###################");
			SDFSLogger
					.getLog()
					.info("########### Pre-Allocation may take a while ####################################");
			byte[] FREE = new byte[32768 * 4];
			Arrays.fill(FREE, (byte) 0);
			this.chunkDataWriter.seek(0);
			long written = 0;
			while (written < length) {
				this.chunkDataWriter.write(FREE);
				written = written + FREE.length;
			}
			SDFSLogger.getLog().info(
					"############ Pre-Allocated Chunkstore to size " + length
							+ "####################");
		}
		this.chunkDataWriter.seek(0);

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
	public void addChunkStoreListener(AbstractChunkStoreListener listener) {
		this.listeners.add(listener.getID(), listener);
	}

	public void close() throws IOException {
		this.closed = true;
		this.closeStore();
		RandomAccessFile raf = new RandomAccessFile(f, "rw");
		raf.getChannel().force(true);
	}

	public static void closeAll() {
		Iterator<FileChunkStore> iter = stores.iterator();
		while (iter.hasNext()) {
			try {
				iter.next().close();
			} catch (IOException e) {

			}
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

}
