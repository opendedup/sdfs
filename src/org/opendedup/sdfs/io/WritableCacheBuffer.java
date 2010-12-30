package org.opendedup.sdfs.io;

import java.io.File;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.util.HashFunctions;
import org.opendedup.util.SDFSLogger;

import org.opendedup.hashing.Tiger16HashEngine;
import org.opendedup.hashing.TigerHashEngine;
import org.opendedup.sdfs.Main;

/**
 * 
 * @author annesam WritableCacheBuffer is used to store written data for later
 *         writing and reading by the DedupFile. WritableCacheBuffers are
 *         evicted from the file system based LRU. When a writable cache buffer
 *         is evicted it is then written to the dedup chunk service
 */
public class WritableCacheBuffer extends DedupChunk {

	private static final long serialVersionUID = 8325202759315844948L;
	private transient static byte[] defaultHash = null;
	private byte[] buf = null;
	private boolean dirty = false;
	private long endPosition = 0;
	private int currentLen = 0;

	private int bytesWritten = 0;
	private DedupFile df;
	private ReentrantLock lock = new ReentrantLock();
	private boolean closed = false;
	File blockFile = null;
	RandomAccessFile raf = null;
	boolean rafInit = false;
	boolean prevDoop = false;
	boolean flushing = false;
	private boolean safeSync = Main.safeSync;

	static {
		try {
			if (Main.hashLength == 16) {
				Tiger16HashEngine he = new Tiger16HashEngine();
				defaultHash = he.getHash(new byte[Main.CHUNK_LENGTH]);
				he.destroy();
			} else {
				TigerHashEngine he = new TigerHashEngine();
				defaultHash = he.getHash(new byte[Main.CHUNK_LENGTH]);
				he.destroy();
			}
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error initializing WritableCacheBuffer",
					e);
		}
	}

	protected WritableCacheBuffer(byte[] hash, long startPos, int length,
			DedupFile df) throws IOException {
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
		this.currentLen = 0;
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
	
	public void flush(boolean flushing) {
		this.lock.lock();
		this.flushing = flushing;
		this.lock.unlock();
	}
	
	public boolean isFlushing() {
		this.lock.lock();
		try {
		return this.flushing;
		}finally{
			this.lock.unlock();
		}
	}

	protected WritableCacheBuffer(DedupChunk dk, DedupFile df)
			throws IOException {
		super(dk.getHash(), dk.getFilePosition(), dk.getLength(), dk
				.isNewChunk());
		this.df = df;
		buf = new byte[Main.CHUNK_LENGTH];
		if (safeSync) {
			blockFile = new File(df.getDatabaseDirPath() + File.separator
					+ dk.getFilePosition() + ".chk");
			if (blockFile.exists()) {
				SDFSLogger.getLog().warn(
						"recovering from unexpected close at "
								+ dk.getFilePosition());
				buf = readBlockFile();
			}
			if (dk.isNewChunk())
				rafInit = true;
		}
		if (!dk.isNewChunk()) {
			try {
				byte[] ck = dk.getChunk();
				if (ck.length > Main.CHUNK_LENGTH) {
					SDFSLogger.getLog().info(
							"Alert ! returned chunk to large " + ck.length
									+ " > " + Main.CHUNK_LENGTH);
					buf = ck;
				} else {
					this.currentLen = ck.length;
					buf = ck;
				}
			} catch (Exception e) {
				buf = new byte[Main.CHUNK_LENGTH];
				SDFSLogger.getLog().fatal(
						"unable to get chunk bytes for " + dk.getHash()
								+ " at position " + dk.getFilePosition(), e);
			}
		}
		// this.currentLen = 0;
		if (Arrays.equals(dk.getHash(), defaultHash)) {
			this.currentLen = 0;
		}
		this.setLength(buf.length);
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

	public int capacity() {
		return this.buf.length;
	}

	public int position() {
		return this.currentLen;
	}

	public long getEndPosition() {
		return endPosition;
	}

	public byte[] getChunk() throws IOException {
		this.lock.lock();
		try {
			return buf;
		} finally {
			this.lock.unlock();
		}
	}

	public int getCurrentLen() {
		return currentLen;
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
			if (this.closed || this.flushing)
				throw new BufferClosedException("Buffer Closed");
			/*
			 * if(pos != 0) SDFSLogger.getLog().info("start at " + pos);
			 * if(b.length != this.capacity())
			 * SDFSLogger.getLog().info("!capacity " + b.length);
			 */

			if (pos > buf.length) {
				byte[] _b = new byte[Main.CHUNK_LENGTH];
				System.arraycopy(buf, 0, _b, 0, buf.length);
				buf = _b;
			}
			ByteBuffer _buff = ByteBuffer.wrap(buf);
			_buff.position(pos);
			_buff.put(b);
			buf = _buff.array();
			if (pos > currentLen)
				this.currentLen = pos;
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
			if (this.closed || this.flushing)
				throw new BufferClosedException("Buffer Closed");
			if (len < this.currentLen) {
				if (!Main.safeSync) {
					byte[] b = new byte[Main.CHUNK_LENGTH];
					ByteBuffer _buf = ByteBuffer.wrap(b);
					_buf.put(buf, 0, len);
					buf = _buf.array();
				} else {
					this.destroy();

				}
			}
			this.setDirty(true);
			this.currentLen = len;
		} finally {
			this.lock.unlock();
		}
	}

	public boolean isDirty() {
		return dirty;
	}

	public void setDirty(boolean dirty) {
		this.dirty = dirty;
	}

	/*
	 * public void destroy() { lock.lock(); if (buf != null) { buf.clear(); buf
	 * = null; } lock.unlock(); }
	 */

	public String toString() {
		return this.getHash() + ":" + this.getFilePosition() + ":"
				+ this.getLength() + ":" + this.getEndPosition();
	}

	protected void open() {
		try {
			this.lock.lock();
			this.flushing = false;
			this.closed = false;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Error while opening");
			throw new IllegalArgumentException("error");
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

	public void close() {
		try {
			this.lock.lock();
			if(!this.flushing)
				SDFSLogger.getLog().info(this.getFilePosition() + " not flushing");
			else if (!this.closed) {
				this.closed = true;
				this.df.writeCache(this, false);
			}else {
				SDFSLogger.getLog().info(this.getFilePosition() + " already closed");
			}
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Error while closing", e);
			throw new IllegalArgumentException("error while closing "
					+ e.toString());
		} finally {
			this.closed = false;
			this.flushing = false;
			this.lock.unlock();
		}
	}

	public void persist() {
		try {
			this.lock.lock();
			this.df.writeCache(this, true);
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
					e.printStackTrace();
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
			return HashFunctions.getMurmurHashCode(buf);
		} finally {
			this.lock.unlock();
		}
	}

}
