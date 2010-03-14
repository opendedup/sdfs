package org.opendedup.sdfs.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.util.logging.*;

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
	private transient static Logger log = Logger.getLogger("sdfs");
	private int bytesWritten = 0;
	private DedupFile df;
	private ReentrantLock lock = new ReentrantLock();
	private boolean closed;
	File blockFile = null;
	RandomAccessFile raf = null;
	boolean rafInit = false;
	static {
		try {
			defaultHash = HashFunctions.getSHAHashBytes(new byte[Main.CHUNK_LENGTH]);
		}catch(Exception e) {
			log.log(Level.SEVERE,"error initializing WritableCacheBuffer",e);
		}
	}

	protected WritableCacheBuffer(byte[] hash, long startPos, int length,
			DedupFile df) throws IOException {
		super(hash, startPos, length, true);
		this.df = df;
		buf = ByteBuffer.wrap(new byte[Main.CHUNK_LENGTH]);
		if (Main.safeSync) {
			blockFile = new File(df.getDatabaseDirPath() + File.separator
					+ startPos + ".chk");
			if (blockFile.exists()) {
				log.warning("recovering from unexpected close at " + startPos);
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
		if (Main.safeSync) {
			blockFile = new File(df.getDatabaseDirPath() + File.separator
					+ dk.getFilePosition() + ".chk");
			if (blockFile.exists()) {
				log.warning("recovering from unexpected close at "
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
					log.info("Alert ! returned chunk to large " + ck.length
							+ " > " + Main.CHUNK_LENGTH);
					buf = ByteBuffer.wrap(ck);
				} else {
					buf.put(ck);
				}
			} catch (Exception e) {
				buf.put(new byte[Main.CHUNK_LENGTH]);
				log.log(Level.SEVERE,
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
		if(Main.safeSync) {
			try {
				lock.lock();
			
			raf = new RandomAccessFile(blockFile, "rw");
			raf.getChannel().force(false);
			raf.close();
			raf = null;
			lock.unlock();
			return true;
			
			}catch(Exception e) {
				if(lock.isLocked())
					lock.unlock();
				log.log(Level.WARNING, "unable to sync " + this.blockFile.getPath(),e);
				throw new IOException(e.toString());
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

	private void position(int pos) {
		this.buf.position(pos);
	}

	private void setCurrentLen(int currentLen) {
		this.currentLen = currentLen;
	}

	public long getEndPosition() {
		return endPosition;
	}

	private void setEndPosition(long endPosition) {
		this.endPosition = endPosition;
	}

	public byte[] getChunk() throws IOException {
		byte b[] = new byte[Main.CHUNK_LENGTH];
		try {
			lock.lock();
			// log.info("Buffer pos :" + buf.position() + " len :" +
			// buf.capacity());
			buf.position(0);
			buf.get(b);

		} catch (Exception e) {
			if (lock.isLocked())
				lock.unlock();
			throw new IOException(e.toString());
		} finally {
			if (lock.isLocked())
				lock.unlock();
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
			lock.lock();
			buf.position(pos);
			buf.put(b);
			if (buf.position() > currentLen)
				this.currentLen = buf.position();
			if (Main.safeSync) {
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
			lock.unlock();
			log.severe("Error while writing data [" + b.length + "] position ["
					+ pos + "]");
			throw new IllegalArgumentException("error");
		} finally {
			lock.unlock();
		}
	}

	public void truncate(int len) {

		try {
			lock.lock();
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
			lock.unlock();
		} catch (Exception e) {
			if (lock.isLocked())
				lock.unlock();
			log.log(Level.SEVERE, "Error while truncating " + len, e);
			throw new IllegalArgumentException("error");
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
			lock.lock();
			this.closed = false;
		} catch (Exception e) {
			lock.unlock();
			log.severe("Error while opening");
			throw new IllegalArgumentException("error");
		} finally {
			lock.unlock();
		}
	}

	protected boolean isClosed() {
		return this.closed;
	}

	public void close() {
		try {
			lock.lock();
			this.df.writeCache(this, false);
			this.closed = true;
		} catch (Exception e) {
			if (lock.isLocked())
				lock.unlock();
			log.log(Level.SEVERE, "Error while closing", e);
			throw new IllegalArgumentException("error while closing "
					+ e.toString());
		} finally {
			if (lock.isLocked())
				lock.unlock();
		}
	}

	public void destroy() {
		if (raf != null) {
			try {
				lock.lock();
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
				lock.unlock();
			}
		}
		if (this.blockFile != null) {
			blockFile.delete();
			blockFile = null;
		}
	}

}
