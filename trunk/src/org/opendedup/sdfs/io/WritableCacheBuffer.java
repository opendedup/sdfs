package org.opendedup.sdfs.io;

import java.io.File;


import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.util.SDFSLogger;

import org.opendedup.hashing.MurmurHash3;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.HCServiceProxy;

/**
 * 
 * @author annesam WritableCacheBuffer is used to store written data for later
 *         writing and reading by the DedupFile. WritableCacheBuffers are
 *         evicted from the file system based LRU. When a writable cache buffer
 *         is evicted it is then written to the dedup chunk service
 */
public class WritableCacheBuffer extends DedupChunk {

	private static final long serialVersionUID = 8325202759315844948L;
	private byte[] buf = null;
	private boolean dirty = false;

	private long endPosition = 0;
	// private int currentLen = 0;

	private int bytesWritten = 0;
	private SparseDedupFile df;
	private final ReentrantLock lock = new ReentrantLock();
	private boolean closed = true;
	private boolean flushing = true;
	File blockFile = null;
	RandomAccessFile raf = null;
	boolean rafInit = false;
	boolean prevDoop = false;
	private boolean safeSync = Main.safeSync;

	static {

	}

	protected WritableCacheBuffer(byte[] hash, long startPos, int length,
			SparseDedupFile df) throws IOException {
		super(hash, startPos, length, true);
		this.df = df;
		buf = new byte[Main.CHUNK_LENGTH];
		if (safeSync) {
			blockFile = new File(df.getDatabaseDirPath() + File.separator
					+ startPos + ".chk");
			if (blockFile.exists()) {
				SDFSLogger.getLog().warn(
						"recovering from unexpected close at " + startPos);
				buf = readBlockFile();
			}
			this.rafInit = true;
		}
		// this.currentLen = 0;
		this.setLength(length);
		this.endPosition = this.getFilePosition() + this.getLength();
		this.setWritable(true);
	}

	protected WritableCacheBuffer(long startPos) throws IOException {
		super(startPos);
	}

	private byte[] readBlockFile() throws IOException {
		raf = new RandomAccessFile(blockFile, "r");
		byte[] b = new byte[(int) raf.length()];
		raf.read(b);
		raf.close();
		raf = null;
		return b;
	}

	public int getBytesWritten() {
		return bytesWritten;
	}

	public DedupFile getDedupFile() {
		return this.df;
	}

	protected WritableCacheBuffer(DedupChunk dk, SparseDedupFile df)
			throws IOException {
		super(dk.getHash(), dk.getFilePosition(), dk.getLength(), dk
				.isNewChunk());
		this.df = df;
		if (this.isNewChunk())
			buf = new byte[Main.CHUNK_LENGTH];
		if (safeSync) {
			blockFile = new File(df.getDatabaseDirPath() + File.separator
					+ this.getFilePosition() + ".chk");
			if (blockFile.exists()) {
				SDFSLogger.getLog().warn(
						"recovering from unexpected close at "
								+ this.getFilePosition());
				buf = readBlockFile();
			}
			if (this.isNewChunk())
				rafInit = true;
		}
		this.setLength(Main.CHUNK_LENGTH);
		this.endPosition = this.getFilePosition() + this.getLength();
		this.setWritable(true);
	}

	public boolean sync() throws IOException {
		if (safeSync) {
			try {
				this.lock.lock();
				raf = new RandomAccessFile(blockFile, "rw");
				raf.getChannel().force(false);
				raf.close();
				raf = null;
				return true;

			} catch (Exception e) {
				SDFSLogger.getLog().warn(
						"unable to sync " + this.blockFile.getPath(), e);
				throw new IOException(e);
			} finally {
				this.lock.unlock();
			}
		}
		return false;
	}

	private void initBuffer() {
		if (this.buf == null) {
			try {
				buf = HCServiceProxy.fetchChunk(this.getHash());
				if (buf.length > Main.CHUNK_LENGTH) {
					SDFSLogger.getLog().info(
							"Alert ! returned chunk to large " + buf.length
									+ " > " + Main.CHUNK_LENGTH);
				}
			} catch (Exception e) {
				buf = new byte[Main.CHUNK_LENGTH];
				SDFSLogger.getLog().fatal(
						"unable to get chunk bytes for " + this.getHash()
								+ " at position " + this.getFilePosition(), e);
			}
		}
	}

	public int capacity() {
		this.lock.lock();
		try {
			if (buf != null) {
				return this.buf.length;
			} else {
				return Main.CHUNK_LENGTH;
			}
		} finally {
			this.lock.unlock();
		}
	}

	public long getEndPosition() {
		return endPosition;
	}

	public byte[] getChunk() throws IOException, BufferClosedException {
		this.lock.lock();
		if (this.closed)
			throw new BufferClosedException("Buffer Closed");
		if (this.flushing)
			throw new BufferClosedException("Buffer Flushing");
		try {
			this.initBuffer();
			return buf;
		} finally {
			this.lock.unlock();
		}
	}

	/**
	 * Writes to the given target array
	 * 
	 * @param b
	 *            the source array
	 * @param pos
	 *            the position within the target array to write to
	 * @param len
	 *            the length to write from the target array
	 * @throws BufferClosedException
	 * @throws IOException
	 */

	public void write(byte[] b, int pos) throws BufferClosedException,
			IOException {
		this.lock.lock();
		try {
			if (this.closed)
				throw new BufferClosedException("Buffer Closed while writing");
			if (this.flushing)
				throw new BufferClosedException("Buffer Flushing");
			/*
			 * if(pos != 0) SDFSLogger.getLog().info("start at " + pos);
			 * if(b.length != this.capacity())
			 * SDFSLogger.getLog().info("!capacity " + b.length);
			 */
			if (pos == 0 && b.length == Main.CHUNK_LENGTH) {
				this.buf = b;
			} else {
				this.initBuffer();
				ByteBuffer _buff = ByteBuffer.wrap(buf);
				_buff.position(pos);
				_buff.put(b);
				buf = _buff.array();
			}
			if (safeSync) {
				raf = new RandomAccessFile(blockFile, "rw");
				if (!this.rafInit) {
					raf.seek(0);
					raf.write(buf);
					this.rafInit = true;
				}
				raf.seek(pos);
				raf.write(b);
				raf.close();
				raf = null;
			}
			this.setDirty(true);
			this.bytesWritten = this.bytesWritten + b.length;
		} finally {
			this.lock.unlock();
		}
	}

	public void truncate(int len) throws BufferClosedException {
		try {
			this.lock.lock();
			if (this.closed)
				throw new BufferClosedException("Buffer Closed");
			if (!Main.safeSync) {
				byte[] b = new byte[Main.CHUNK_LENGTH];
				ByteBuffer _buf = ByteBuffer.wrap(b);
				_buf.put(buf, 0, len);
				buf = _buf.array();
			} else {
				this.destroy();
			}
			this.setDirty(true);
		} finally {
			this.lock.unlock();
		}
	}

	public boolean isDirty() {
		this.lock.lock();
		try {
			return dirty;
		} finally {
			this.lock.unlock();
		}
	}

	public void setDirty(boolean dirty) {
		this.lock.lock();
		this.dirty = dirty;
		this.lock.unlock();
	}

	public String toString() {
		return this.getHash() + ":" + this.getFilePosition() + ":"
				+ this.getLength() + ":" + this.getEndPosition();
	}

	public void open() {
		try {
			this.lock.lock();
			if (this.closed || this.flushing) {
				this.closed = false;
				this.flushing = false;
				df.putBufferIntoWrite(this);
			}
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Error while opening");
			throw new IllegalArgumentException("error");
		} finally {
			this.lock.unlock();
		}
	}

	protected void flush() throws BufferClosedException {
		try {
			this.lock.lock();
			if (this.flushing) {
				SDFSLogger.getLog().debug(
						"cannot flush buffer at pos " + this.getFilePosition()
								+ " already flushing");
				throw new BufferClosedException("Buffer Closed");
			}
			if (this.closed) {
				SDFSLogger.getLog().debug(
						"cannot flush buffer at pos " + this.getFilePosition()
								+ " closed");
				throw new BufferClosedException("Buffer Closed");
			}
			this.flushing = true;
			this.df.putBufferIntoFlush(this);
			SparseDedupFile.pool.execute(this);
		} finally {
			this.lock.unlock();
		}
	}

	protected boolean isClosed() {
		this.lock.lock();
		try {
			return this.closed;
		} finally {
			this.lock.unlock();
		}
	}

	public void close() throws IOException {
		try {
			this.lock.lock();
			if (!this.flushing)
				SDFSLogger.getLog().debug(
						"####" + this.getFilePosition() + " not flushing");
			else if (this.closed) {
				SDFSLogger.getLog().info(
						this.getFilePosition() + " already closed");
			} else {

				this.df.writeCache(this);
				WritableCacheBuffer _wb = this.df.flushingBuffers.remove(this
						.getFilePosition());
				if (_wb == null) {
					SDFSLogger.getLog().debug(
							this.getFilePosition()
									+ " not found in flushing buffer");
				}
				this.closed = true;
				this.flushing = false;
			}
		} catch (Exception e) {
			throw new IOException(e);
		} finally {

			this.lock.unlock();
		}
	}

	protected byte[] getFlushedBuffer() throws BufferClosedException {
		this.lock.lock();
		try {
			if (this.closed) {
				SDFSLogger.getLog().info(
						this.getFilePosition() + " already closed");
				throw new BufferClosedException("Buffer Closed");
			}
			if (this.buf == null)
				SDFSLogger.getLog().info(
						this.getFilePosition() + " buffer is null");
			return this.buf;
		} finally {
			this.lock.unlock();
		}
	}

	public void persist() {
		try {
			this.lock.lock();
			this.df.writeCache(this);
			this.closed = true;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Error while closing", e);
			throw new IllegalArgumentException("error while closing "
					+ e.toString());
		} finally {
			this.lock.unlock();
		}
	}

	public void destroy() {
		if (raf != null) {
			try {
				this.lock.lock();
				try {
					raf.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					SDFSLogger.getLog().info(
							"error while destroying write buffer ", e);
				}
			} catch (Exception e) {
			} finally {
				this.lock.unlock();
			}
		}
		if (this.blockFile != null) {
			blockFile.delete();
			blockFile = null;
		}
	}

	public boolean isPrevDoop() {
		return prevDoop;
	}

	public void setPrevDoop(boolean prevDoop) {
		this.prevDoop = prevDoop;
	}

	public int hashCode() {
		this.lock.lock();
		try {
			return MurmurHash3.MurmurHash3_x64_32(buf,6442);
		} finally {
			this.lock.unlock();
		}
	}

}
