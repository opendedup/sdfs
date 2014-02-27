package org.opendedup.sdfs.filestore;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
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
import org.opendedup.util.StringUtils;
import org.w3c.dom.Element;

public class VariableFileChunkStore implements AbstractChunkStore {
	private final long pageSize = (long) Main.chunkStorePageSize;
	private final int iPageSize = 4 + 4 + 8 + 1 + 1
			+ HashFunctionPool.hashLength;
	private boolean closed = false;
	private static File chunk_location = new File(Main.chunkStore);
	private int[] storeLengths = FactorTest.factorsOf(Main.chunkStorePageSize);
	private FileChunkStore[] st = new FileChunkStore[storeLengths.length];
	private FileChannel fc = null;
	private RandomAccessFile chunkDataWriter = null;
	private OpenBitSet freeSlots = null;
	File f;
	Path p;
	private long currentLength = 0;
	private String name;
	private byte[] FREE = new byte[(int) pageSize];
	private FileChannel iterFC = null;
	private AbstractHashEngine hc = null;
	private File bsf;
	private File lsf;
	private FCPool pool = null;
	private AtomicLong size = new AtomicLong(0);

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
			lsf = new File(chunk_location + File.separator + "sizemarker.lng");
			if (!lsf.exists()) {
				this.size = new AtomicLong(0);
			} else {
				try {
					RandomAccessFile rf = new RandomAccessFile(lsf, "rw");
					rf.seek(0);
					this.size = new AtomicLong(rf.readLong());
					rf.close();
					rf = null;
					lsf.delete();
				} catch (Exception e) {
					SDFSLogger.getLog().error(
							"Unable to load filestore size from "
									+ lsf.getPath(), e);
				}
				SDFSLogger.getLog().info(
						"FileStore Size [" + this.size.get() + "] ");
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
			pool = new FCPool(f, 100);
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
				store.setName(Integer.toString(storeLengths[i]));
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
				this.pool.close();
			} catch (Exception e) {

			}
			try {
				if (this.freeSlots != null) {
					OpenBitSetSerialize.writeOut(bsf.getPath(), this.freeSlots);
					SDFSLogger.getLog().info("Persisted Free Slots");
					this.freeSlots = null;
				}
			} catch (Exception e) {
			}
			try {
				RandomAccessFile rf = new RandomAccessFile(lsf, "rw");
				rf.seek(0);
				rf.writeLong(this.size.get());
				SDFSLogger.getLog().info("Persisted FileSize");

				rf.close();
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
		return size.get();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.filestore.AbstractChunkStore#bytesRead()
	 */
	@Override
	public long bytesRead() {
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.sdfs.filestore.AbstractChunkStore#bytesWritten()
	 */
	@Override
	public long bytesWritten() {
		return 0;
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
		FileChannel rf = null;
		try {

			buf = ByteBuffer.allocate(iPageSize);
			byte[] b = new byte[chunk.length];
		    System.arraycopy(chunk, 0, b, 0, chunk.length);
			
			byte[] data = null;
			boolean compress = Main.compress;
			boolean encrypt = false;
			if (Main.compress) {
				data = CompressionUtils.compressLz4(b);
				if (data.length >= b.length) {
					data = b;
					compress = false;
				} else {
					compress = true;
				}
			}
			if (Main.chunkStoreEncryptionEnabled) {
				data = EncryptUtils.encrypt(data);
				encrypt = true;
			}
			
			FileChunkStore store = this.getStore(data.length);
			long ipos = store.writeChunk(hash, data, data.length);
			// SDFSLogger.getLog().info("#######3 writing data from ["
			// +data.length+"] [" + ipos +"] comp=" + compress + " enc=" +
			// encrypt + " store=" +store.getName() );
			long pos = -1;
			reservePositionlock.lock();
			if (this.freeSlots != null) {
				pos = this.freeSlots.nextSetBit(0);
				if (pos < 0) {
					this.freeSlots = null;
				} else {
					this.freeSlots.clear(pos);
					pos = pos * (long) this.iPageSize;
				}
			}
			if (pos < 0) {
				pos = this.currentLength;
				this.currentLength = this.currentLength + this.iPageSize;
			}
			reservePositionlock.unlock();

			byte comp = 1;
			byte enc = 0;
			if (!compress)
				comp = 0;
			if (encrypt)
				enc = 1;
			buf.putLong(ipos);
			buf.putInt(b.length);
			buf.putInt(data.length);
			buf.put(comp);
			buf.put(enc);
			buf.put(hash);
			buf.position(0);
			rf = pool.borrowObject();
			rf.write(buf, pos);
			this.size.addAndGet(b.length);
			return pos;
		} catch (Throwable e) {
			SDFSLogger.getLog().fatal("unable to write data ", e);
			throw new IOException("unable to write data ");
		} finally {
			try {
				pool.returnObject(rf);
			} catch (Exception e) {
			}
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

		FileChunkStore store = null;
		ByteBuffer buf = ByteBuffer.wrap(new byte[this.iPageSize]);
		byte comp = 0;
		byte enc = 0;
		FileChannel rf = pool.borrowObject();
		int iLen = 0;
		int cLen = 0;
		long iStart = 0;
		try {
			rf.read(buf, start);
			buf.position(0);
			iStart = buf.getLong();
			cLen = buf.getInt();
			iLen = buf.getInt();
			comp = buf.get();
			enc = buf.get();
			store = this.getStore(iLen);
			byte[] chunk = store.getChunk(hash, iStart, iLen);
			// SDFSLogger.getLog().info("getting data from [" +iLen+"] [" +
			// iStart +"] comp=" + comp + " enc=" + enc + " store="
			// +store.getName());
			if (enc == 1)
				chunk = EncryptUtils.decrypt(chunk);
			if (comp == 1)
				chunk = CompressionUtils.decompressLz4(chunk, cLen);
			return chunk;
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to fetch chunk at position " + start + " size="
							+ store.getName() + " comp=" + comp + " enc=" + enc
							+ " cspos=" + iStart + " chunklen=" + cLen
							+ " compressed clen=" + iLen, e);
			throw new IOException(e);
		} finally {
			try {
				pool.returnObject(rf);
			} catch (Exception e) {
			}
		}
	}

	@Override
	public void deleteChunk(byte[] hash, long start, int len)
			throws IOException {
		FileChannel rf = pool.borrowObject();
		try {
			ByteBuffer buf = ByteBuffer.allocate(iPageSize);

			rf.read(buf, start);
			buf.position(0);
			long iStart = buf.getLong();
			int cLen = buf.getInt();
			int iLen = buf.getInt();
			FileChunkStore store = this.getStore(iLen);
			store.deleteChunk(hash, iStart, iLen);
			this.size.addAndGet(-1*cLen);
			
		} finally {
			pool.returnObject(rf);
		}
		reservePositionlock.lock();
		try {
			if (this.closed)
				throw new IOException("ChunkStore is closed");
			if (this.freeSlots == null)
				this.freeSlots = new OpenBitSet();
			this.freeSlots.set(start / ((long) this.iPageSize));

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
	public void setSize(long size) {
		// TODO Auto-generated method stub

	}

	@Override
	public ChunkData getNextChunck() throws IOException {
		if (iterFC.position() >= iterFC.size()) {
			iterFC.close();
			return null;
		}
		FileChunkStore store = null;
		ByteBuffer buf = ByteBuffer.wrap(new byte[this.iPageSize]);
		long pos = -1;
		byte comp = 0;
		byte enc = 0;
		try {
			pos = iterFC.position();
			buf.position(0);
			iterFC.read(buf);
			buf.position(0);
			long iStart = buf.getLong();
			int cLen = buf.getInt();
			this.size.addAndGet(cLen);
			int iLen = buf.getInt();
			comp = buf.get();
			enc = buf.get();
			byte[] _hash = new byte[HashFunctionPool.hashLength];
			buf.get(_hash);
			store = this.getStore(iLen);
			byte[] chunk = store.getChunk(_hash, iStart, iLen);
			// SDFSLogger.getLog().info("getting data from [" +iLen+"] [" +
			// iStart +"] comp=" + comp + " enc=" + enc + " store="
			// +store.getName());
			if (enc == 1)
				chunk = EncryptUtils.decrypt(chunk);
			if (comp == 1)
				chunk = CompressionUtils.decompressLz4(chunk, cLen);
			byte[] hash = hc.getHash(chunk);
			if (!Arrays.areEqual(_hash, hash))
				SDFSLogger.getLog().warn(
						"possible data corruption at " + pos + " hash="
								+ StringUtils.getHexString(hash) + " expected="
								+ StringUtils.getHexString(_hash));
			ChunkData chk = new ChunkData(_hash, pos);
			chk.setChunk(chunk);
			chk.cLen = chunk.length;
			return chk;
		} catch (Exception e) {
			SDFSLogger
					.getLog()
					.error("unable to fetch chunk at position " + pos + "size="
							+ store.getName() + " comp=" + comp + " enc=" + enc,
							e);
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
		if (this.freeSlots != null) {
			return this.freeSlots.cardinality();
		} else
			return 0;
	}

}
