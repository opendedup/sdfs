package org.opendedup.sdfs.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.hashing.MurmurHash3;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.HCServiceProxy;

/**
 * 
 * @author annesam WritableCacheBuffer is used to store written data for later
 *         writing and reading by the DedupFile. WritableCacheBuffers are
 *         evicted from the file system based LRU. When a writable cache buffer
 *         is evicted it is then written to the dedup chunk service
 */
public class WritableCacheBuffer implements DedupChunkInterface {

	private byte[] buf = null;
	private boolean dirty = false;

	private long endPosition = 0;
	// private int currentLen = 0;
	private byte[] hash;
	private int length;
	private long position;
	private boolean newChunk = false;
	private boolean writable = false;
	private int doop = 0;
	private int bytesWritten = 0;
	private DedupFile df;
	private final ReentrantLock lock = new ReentrantLock();
	private boolean closed = true;
	private boolean flushing = true;
	File blockFile = null;
	RandomAccessFile raf = null;
	boolean rafInit = false;
	int prevDoop = 0;
	private boolean safeSync = false;
	private byte[] hashloc;
	private boolean batchprocessed;
	private boolean batchwritten;

	static {

	}

	public WritableCacheBuffer(byte[] hash, long startPos, int length,
			DedupFile df, byte[] hashloc) throws IOException {
		this.hash = hash;
		this.length = length;
		this.position = startPos;
		this.newChunk = true;
		this.hashloc = hashloc;
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

	

	private byte[] readBlockFile() throws IOException {
		raf = new RandomAccessFile(blockFile, "r");
		byte[] b = new byte[(int) raf.length()];
		raf.read(b);
		raf.close();
		raf = null;
		return b;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#getBytesWritten()
	 */
	@Override
	public int getBytesWritten() {
		return bytesWritten;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#getDedupFile()
	 */
	@Override
	public DedupFile getDedupFile() {
		return this.df;
	}

	private int currentPos = 1;

	public void resetHashLoc() {
		this.hashloc = new byte[8];
		this.hashloc[0] = -1;
	}

	public synchronized void addHashLoc(byte loc) {
		// SDFSLogger.getLog().info("set " + this.currentPos + " to " + loc);
		if (currentPos < this.hashloc.length) {
			if (this.hashloc[0] == -1)
				this.hashloc[0] = 0;
			this.hashloc[currentPos] = loc;
			this.currentPos++;
		}
	}

	public WritableCacheBuffer(DedupChunkInterface dk, DedupFile df)
			throws IOException {
		this.hash = dk.getHash();
		this.position = dk.getFilePosition();
		this.length = dk.getLength();
		this.newChunk = dk.isNewChunk();
		this.hashloc = dk.getHashLoc();
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#sync()
	 */
	@Override
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

	public byte[] getReadChunk() throws IOException {
		this.lock.lock();
		try {
			this.initBuffer();
			return buf;
		} finally {
			this.lock.unlock();
		}
	}

	private void initBuffer() {
		if (this.buf == null) {
			try {
				if(HashFunctionPool.max_hash_cluster > 1) {
				ByteBuffer hcb = ByteBuffer.wrap(new byte[Main.CHUNK_LENGTH]);
				ByteBuffer hb = ByteBuffer.wrap(this.getHash());
				ByteBuffer hl = ByteBuffer.wrap(this.hashloc);
				for(int i = 0;i < HashFunctionPool.max_hash_cluster;i++) {
					byte [] _hash = new byte[HashFunctionPool.hashLength];
					byte [] _hl = new byte[8];
					hl.get(_hl);
					
					hb.get(_hash);
					if(_hl[1] != 0)
						hcb.put(HCServiceProxy.fetchChunk(_hash, _hl));
					else
						break;
				}
				this.buf = hcb.array();
				}else {
					this.buf = HCServiceProxy.fetchChunk(this.getHash(), this.hashloc);
				}
			} catch (Exception e) {
				buf = new byte[Main.CHUNK_LENGTH];
				SDFSLogger.getLog().fatal(
						"unable to get chunk bytes for " + this.getHash()
								+ " at position " + this.getFilePosition(), e);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#capacity()
	 */
	@Override
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#getEndPosition()
	 */
	@Override
	public long getEndPosition() {
		return endPosition;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#getChunk()
	 */
	@Override
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#write(byte[], int)
	 */

	@Override
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#truncate(int)
	 */
	@Override
	public void truncate(int len) throws BufferClosedException {
		try {
			this.lock.lock();
			if (this.closed)
				throw new BufferClosedException("Buffer Closed");
			if (!safeSync) {
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#isDirty()
	 */
	@Override
	public boolean isDirty() {
		this.lock.lock();
		try {
			return dirty;
		} finally {
			this.lock.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#setDirty(boolean)
	 */
	@Override
	public void setDirty(boolean dirty) {
		this.lock.lock();
		this.dirty = dirty;
		this.lock.unlock();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#toString()
	 */
	@Override
	public String toString() {
		return this.getHash() + ":" + this.getFilePosition() + ":"
				+ this.getLength() + ":" + this.getEndPosition();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#open()
	 */
	@Override
	public void open() {
		try {
			this.lock.lock();
			if (this.closed || this.flushing) {
				this.closed = false;
				this.flushing = false;
			}
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Error while opening", e);
			throw new IllegalArgumentException("error");
		} finally {
			this.lock.unlock();
		}
	}

	public void flush() throws BufferClosedException {
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
			if (this.isDirty()) {
				this.df.putBufferIntoFlush(this);
				SparseDedupFile.pool.execute(this);
			}
		} finally {
			this.lock.unlock();
		}
	}

	public boolean isClosed() {
		this.lock.lock();
		try {
			return this.closed;
		} finally {
			this.lock.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#close()
	 */
	@Override
	public void close() throws IOException {
		try {
			this.lock.lock();
			
			if (!this.flushing)
				SDFSLogger.getLog().debug(
						"####" + this.getFilePosition() + " not flushing");
						
			else if (this.closed) {
				SDFSLogger.getLog().debug(
						this.getFilePosition() + " already closed");
			} else if(this.dirty){
				this.df.writeCache(this);
				df.removeFromFlush(this.getFilePosition());
				this.closed = true;
				this.flushing = false;
			}
		} catch (Exception e) {
			throw new IOException(e);
		} finally {

			this.lock.unlock();
		}
	}

	public void startClose() {
		this.lock.lock();
		this.batchprocessed = true;
	}

	public boolean isBatchProcessed() {
		return this.batchprocessed;
	}

	public void endClose() throws IOException {
		try {
			if (!this.flushing)
				SDFSLogger.getLog().debug(
						"####" + this.getFilePosition() + " not flushing");
			else if (this.closed) {
				SDFSLogger.getLog().debug(
						this.getFilePosition() + " already closed");
			} else {
				this.df.writeCache(this);
				df.removeFromFlush(this.getFilePosition());
				this.closed = true;
				this.flushing = false;
			}
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			this.batchprocessed = false;
			this.lock.unlock();
		}
	}

	public byte[] getFlushedBuffer() throws BufferClosedException {
		this.lock.lock();
		try {
			if (this.closed) {
				SDFSLogger.getLog().debug(
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#persist()
	 */
	@Override
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#destroy()
	 */
	@Override
	public void destroy() {
		if (raf != null) {
			try {
				this.lock.lock();
				try {
					raf.close();
				} catch (IOException e) {
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#isPrevDoop()
	 */
	@Override
	public int getPrevDoop() {
		return prevDoop;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#setPrevDoop(boolean)
	 */
	@Override
	public void setPrevDoop(int prevDoop) {
		this.prevDoop = prevDoop;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#hashCode()
	 */
	@Override
	public int hashCode() {
		this.lock.lock();
		try {
			return MurmurHash3.MurmurHash3_x64_32(buf, 6442);
		} finally {
			this.lock.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#getHash()
	 */
	@Override
	public byte[] getHash() {
		return this.hash;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#getLength()
	 */
	@Override
	public int getLength() {
		return this.length;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#getFilePosition()
	 */
	@Override
	public long getFilePosition() {
		return this.position;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#setLength(int)
	 */
	@Override
	public void setLength(int length) {
		this.length = length;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#isNewChunk()
	 */
	@Override
	public boolean isNewChunk() {
		return this.newChunk;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#setNewChunk(boolean)
	 */
	@Override
	public void setNewChunk(boolean newChunk) {
		this.newChunk = newChunk;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#setWritable(boolean)
	 */
	@Override
	public void setWritable(boolean writable) {
		this.writable = writable;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#isWritable()
	 */
	@Override
	public boolean isWritable() {
		return this.writable;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#setDoop(boolean)
	 */
	@Override
	public void setDoop(int doop) {
		this.doop = doop;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.CacheBufferInterface2#isDoop()
	 */
	@Override
	public int getDoop() {
		return this.doop;
	}

	@Override
	public byte[] getHashLoc() {
		return this.hashloc;
	}

	@Override
	public void setHashLoc(byte[] hashloc) {
		this.hashloc = hashloc;
	}

	public void setHash(byte[] hash) {
		this.hash = hash;
	}

	public boolean isBatchwritten() {
		return batchwritten;
	}

	public void setBatchwritten(boolean batchwritten) {
		this.batchwritten = batchwritten;
	}

}
