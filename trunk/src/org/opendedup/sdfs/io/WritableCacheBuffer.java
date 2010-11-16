package org.opendedup.sdfs.io;

import java.io.File;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.opendedup.util.SDFSLogger;


import org.opendedup.sdfs.Main;
import org.opendedup.util.HashFunctions;

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
	private ByteBuffer buf = null;
	private boolean dirty = false;
	private long endPosition = 0;
	private int currentLen = 0;
	
	private int bytesWritten = 0;
	private DedupFile df;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private boolean closed;
	File blockFile = null;
	RandomAccessFile raf = null;
	boolean rafInit = false;
	boolean prevDoop = false;
	private boolean safeSync = Main.safeSync;
	

	static {
		try {
			defaultHash = HashFunctions
					.getSHAHashBytes(new byte[Main.CHUNK_LENGTH]);
		} catch (Exception e) {
			SDFSLogger.getLog().fatal( "error initializing WritableCacheBuffer", e);
		}
	}

	protected WritableCacheBuffer(byte[] hash, long startPos, int length,
			DedupFile df) throws IOException {
		super(hash, startPos, length, true);
		this.df = df;
		buf = ByteBuffer.wrap(new byte[Main.CHUNK_LENGTH]);
		if (safeSync) {
			blockFile = new File(df.getDatabaseDirPath() + File.separator
					+ startPos + ".chk");
			if (blockFile.exists()) {
				SDFSLogger.getLog().warn("recovering from unexpected close at " + startPos);
				buf = ByteBuffer.wrap(readBlockFile());
			}
			this.rafInit = true;
		}
		this.currentLen = 0;
		this.setLength(length);
		this.endPosition = this.getFilePosition() + this.getLength();
		this.setWritable(true);
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

	protected WritableCacheBuffer(DedupChunk dk, DedupFile df)
			throws IOException {
		super(dk.getHash(), dk.getFilePosition(), dk.getLength(), dk
				.isNewChunk());
		this.df = df;
		buf = ByteBuffer.wrap(new byte[Main.CHUNK_LENGTH]);
		if (safeSync) {
			blockFile = new File(df.getDatabaseDirPath() + File.separator
					+ dk.getFilePosition() + ".chk");
			if (blockFile.exists()) {
				SDFSLogger.getLog().warn("recovering from unexpected close at "
						+ dk.getFilePosition());
				buf = ByteBuffer.wrap(readBlockFile());
			}
			if (dk.isNewChunk())
				rafInit = true;
		}
		if (!dk.isNewChunk()) {
			try {
				byte[] ck = dk.getChunk();
				if (ck.length > Main.CHUNK_LENGTH) {
					SDFSLogger.getLog().info("Alert ! returned chunk to large " + ck.length
							+ " > " + Main.CHUNK_LENGTH);
					buf = ByteBuffer.wrap(ck);
				} else {
					buf.put(ck);
				}
			} catch (Exception e) {
				buf.put(new byte[Main.CHUNK_LENGTH]);
				SDFSLogger.getLog().fatal(
						"unable to get chunk bytes for " + dk.getHash()
								+ " at position " + dk.getFilePosition(), e);
			}
		}
		// this.currentLen = 0;
		this.currentLen = buf.position();
		if (Arrays.equals(dk.getHash(), defaultHash)) {
			this.currentLen = 0;
		}
		this.setLength(buf.capacity());
		this.endPosition = this.getFilePosition() + this.getLength();
		this.setWritable(true);
	}

	public boolean sync() throws IOException {
		if (safeSync) {
			try {
				this.lock.readLock().lock();

				raf = new RandomAccessFile(blockFile, "rw");
				raf.getChannel().force(false);
				raf.close();
				raf = null;
				return true;

			} catch (Exception e) {
				SDFSLogger.getLog().warn( "unable to sync "
						+ this.blockFile.getPath(), e);
				throw new IOException(e);
			} finally {
				this.lock.readLock().unlock();
			}
		}
		return false;
	}

	public int capacity() {
		return this.buf.capacity();
	}

	public int position() {
		return this.buf.position();
	}

	public long getEndPosition() {
		return endPosition;
	}

	public byte[] getChunk() throws IOException {
		byte b[] = new byte[Main.CHUNK_LENGTH];
		try {
			this.lock.readLock().lock();
			// SDFSLogger.getLog().info("Buffer pos :" + buf.position() + " len :" +
			// buf.capacity());
			buf.position(0);
			buf.get(b);

		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			this.lock.readLock().unlock();
		}
		return b;
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
	 */

	public void write(byte[] b, int pos) throws BufferClosedException {
		if (this.closed)
			throw new BufferClosedException("Buffer Closed");
		try {
			this.lock.writeLock().lock();
			buf.position(pos);
			buf.put(b);
			if (buf.position() > currentLen)
				this.currentLen = buf.position();
			if (safeSync) {
				raf = new RandomAccessFile(blockFile, "rw");
				if (!this.rafInit) {
					raf.seek(0);
					raf.write(buf.array());
					this.rafInit = true;
				}
				raf.seek(pos);
				raf.write(b);
				raf.close();
				raf = null;
			}
			this.setDirty(true);
			this.bytesWritten = this.bytesWritten + b.length;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Error while writing data [" + b.length + "] position ["
					+ pos + "]");
			throw new IllegalArgumentException("error");
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	public void truncate(int len) {

		try {
			this.lock.writeLock().lock();
			if (len < this.currentLen) {
				if (!Main.safeSync) {
					byte[] b = new byte[Main.CHUNK_LENGTH];
					ByteBuffer _buf = ByteBuffer.wrap(b);
					_buf.put(buf.array(), 0, len);
					buf = _buf;
				} else {
					this.destroy();

				}
			}
			this.setDirty(true);
			this.currentLen = len;
		} catch (Exception e) {

			SDFSLogger.getLog().fatal( "Error while truncating " + len, e);
			throw new IllegalArgumentException("error");
		} finally {
			this.lock.writeLock().unlock();
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
			this.lock.writeLock().lock();
			this.closed = false;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Error while opening");
			throw new IllegalArgumentException("error");
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	protected boolean isClosed() {
		this.lock.readLock().lock();
		try {
			return this.closed;
		} catch (Exception e) {
			return this.closed;
		} finally {
			this.lock.readLock().unlock();
		}
	}

	public void close() {
		try {
			this.lock.writeLock().lock();
			this.df.writeCache(this, false);
			this.closed = true;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal( "Error while closing", e);
			throw new IllegalArgumentException("error while closing "
					+ e.toString());
		} finally {
			this.lock.writeLock().unlock();
		}
	}
	
	public void persist() {
		try {
			this.lock.writeLock().lock();
			this.df.writeCache(this,true);
			this.closed = true;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal( "Error while closing", e);
			throw new IllegalArgumentException("error while closing "
					+ e.toString());
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	public void destroy() {
		if (raf != null) {
			try {
				this.lock.writeLock().lock();
				try {
					raf.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				raf = null;
				buf = null;
			} catch (Exception e) {
			} finally {
				this.lock.writeLock().unlock();
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

}
