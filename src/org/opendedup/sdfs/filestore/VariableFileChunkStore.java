package org.opendedup.sdfs.filestore;

import java.io.File;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.util.OpenBitSet;
import org.bouncycastle.util.Arrays;
import org.opendedup.hashing.AbstractHashEngine;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.EncryptUtils;
import org.opendedup.util.FactorTest;
import org.opendedup.util.OpenBitSetSerialize;
import org.w3c.dom.Element;

public class VariableFileChunkStore implements AbstractChunkStore {
	private final long pageSize = (long) Main.chunkStorePageSize;
	private final long lPageSize = 4L + 8L + 1L;
	private final int iPageSize = 4 + 8 + 1;
	private boolean closed = false;
	private static File chunk_location = new File(Main.chunkStore);
	private int[] storeLengths = FactorTest.factorsOf(Main.chunkStorePageSize);
	private FileChunkStore[] st = new FileChunkStore[storeLengths.length];
	private FileChannel fc = null;
	private RandomAccessFile chunkDataWriter = null;
	private OpenBitSet freeSlots = null;
	File f;
	Path p;
	private long currentLength = 0L;
	private String name;

	private byte[] FREE = new byte[(int) pageSize];
	private FileChannel iterFC = null;
	private AbstractHashEngine hc = null;
	private File bsf;

	/**
	 * 
	 * @param name
	 *            the name of the chunk store.
	 */
	public VariableFileChunkStore() {
		SDFSLogger.getLog().info("Opening Variable Length Chunk Store");
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
			SDFSLogger.getLog().info("ChunkStore " + f.getPath() + " created");
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			if (!chunk_location.exists()) {
				chunk_location.mkdirs();
			}
			for (int i = 0; i < storeLengths.length; i++) {
				File f = new File(chunk_location + File.separator + "chunks-"
						+ storeLengths[i]);
				FileChunkStore store = new FileChunkStore(f.getPath(),
						storeLengths[i]);
				st[i] = store;
			}
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
		for (FileChunkStore store : st) {
			try {
				store.close();

			} catch (Exception e) {
			}
			try {
				OpenBitSetSerialize.writeOut(bsf.getPath(), this.freeSlots);
				SDFSLogger.getLog().info("Persisted Free Slots");
				this.freeSlots.clear(0, this.freeSlots.capacity());
			} catch (Exception e) {
			}

		}

	}

	protected void sync() throws IOException {
		for (FileChunkStore store : st) {
			try {
				store.sync();

			} catch (Exception e) {
			}

		}
		try {
			this.fc.force(true);
		} catch (Exception e) {
		}
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
		return (this.currentLength / this.iPageSize) * this.pageSize;
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

	private FileChunkStore getStore(int sz) {
		return this.st[FactorTest.closest2Pos(sz, this.storeLengths)];
	}

	@Override
	public long writeChunk(byte[] hash, byte[] chunk, int len)
			throws IOException {
		if (this.closed)
			throw new IOException("ChunkStore is closed");
		ByteBuffer buf = null;
		try {
			if (Main.chunkStoreEncryptionEnabled)
				chunk = EncryptUtils.encrypt(chunk);
			buf = ByteBuffer.allocate(iPageSize);
			byte[] data = CompressionUtils.compressSnappy(chunk);
			boolean compress = true;
			if (data.length > chunk.length) {
				data = chunk;
				compress = false;
			}
			FileChunkStore store = this.getStore(data.length);
			long ipos = store.writeChunk(hash, data, data.length);
			reservePositionlock.lock();
			long pos = this.freeSlots.nextSetBit(0);
			if (pos < 0) {
				pos = this.currentLength;
				this.currentLength = this.currentLength + this.lPageSize;
			} else {
				this.freeSlots.clear(pos);
				pos = pos * this.lPageSize;
			}
			reservePositionlock.unlock();

			byte comp = 1;
			if (!compress)
				comp = 0;
			buf.putLong(ipos);
			buf.putInt(data.length);
			buf.put(comp);
			buf.position(0);
			fc.write(buf, pos);
			return pos;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("unable to write data ", e);
			throw new IOException("unable to write data ");
		} finally {
			buf = null;
			hash = null;
			chunk = null;
			len = 0;
		}
	}

	@Override
	public byte[] getChunk(byte[] hash, long start, int len) throws IOException {
		if (this.closed)
			throw new IOException("ChunkStore is closed");
		// long time = System.currentTimeMillis();

		ByteBuffer buf = ByteBuffer.allocate(iPageSize);

		try {
			fc.read(buf, start);
			buf.position(0);
			long iStart = buf.getLong();
			int iLen = buf.getInt();
			byte comp = buf.get();
			FileChunkStore store = this.getStore(iLen);
			byte[] chunk = store.getChunk(hash, iStart, iLen);
			if (comp == 1)
				return CompressionUtils.decompressSnappy(chunk);
			else
				return chunk;
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to fetch chunk at position " + start, e);
			throw new IOException(e);
		} finally {
			try {
			} catch (Exception e) {
			}
		}
	}

	@Override
	public void deleteChunk(byte[] hash, long start, int len)
			throws IOException {
		reservePositionlock.lock();
		try {
			if (this.closed)
				throw new IOException("ChunkStore is closed");

			this.freeSlots.set(start / this.pageSize);

		} finally {
			reservePositionlock.unlock();
		}
	}

	@Override
	public void close() {
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
		ByteBuffer buf = ByteBuffer.wrap(new byte[this.iPageSize]);
		long pos = -1;
		try {
			pos = iterFC.position();
			iterFC.read(buf);
			buf.position(0);
			long iStart = buf.getLong();
			int iLen = buf.getInt();
			byte comp = buf.get();
			FileChunkStore store = this.getStore(iLen);
			byte[] chunk = store.getChunk(new byte[16], iStart, iLen);
			if (comp == 1)
				chunk = CompressionUtils.decompressSnappy(chunk);
			byte[] hash = hc.getHash(chunk);
			ChunkData chk = new ChunkData(hash, pos);
			chk.setChunk(chunk);
			return chk;
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
	public long getFreeBlocks() {
		return this.freeSlots.cardinality();
	}

}
