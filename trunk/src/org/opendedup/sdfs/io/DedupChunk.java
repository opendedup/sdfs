package org.opendedup.sdfs.io;

import java.io.IOException;

import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.sdfs.servers.HCServiceProxy;

/**
 * 
 * @author annesam Base class for storing byte arrays associated with dedup
 *         files into memory.
 */
public class DedupChunk implements java.io.Serializable, DedupChunkInterface {
	private static final long serialVersionUID = -5440311151699047048L;
	private byte[] hash;
	private byte[] data = null;
	private int length;
	private long position;
	private boolean newChunk = false;
	private boolean writable = false;
	private boolean doop = false;
	private ReentrantLock lock = new ReentrantLock();
	private byte[] hashloc;

	public DedupChunk(long position) {
		this.position = position;
	}

	/**
	 * 
	 * @param hash
	 *            The MD5 Hash of the chunk requested.
	 * @param position
	 *            The start position within the deduplicated file
	 * @param length
	 *            The length of the chunk
	 */
	public DedupChunk(byte[] hash, long position, int length, boolean newChunk,
			byte[] hashloc) {
		this.hash = hash;
		this.length = length;
		this.position = position;
		this.newChunk = newChunk;
		if (this.isNewChunk())
			data = new byte[this.length];
		else
			this.hashloc = hashloc;
	}

	/**
	 * 
	 * @param hash
	 *            The MD5 Hash of the chunk requested.
	 * @param position
	 *            The start position within the deduplicated file
	 * @param length
	 *            The length of the chunk
	 */
	public DedupChunk(byte[] hash, byte[] data, long position, int length,
			byte[] hashloc) {
		this.hash = hash;
		this.data = data;
		this.length = length;
		this.position = position;
		this.newChunk = false;
		this.hashloc = hashloc;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.DedupChunkInterface#getHash()
	 */

	@Override
	public byte[] getHash() {
		return hash;
	}

	public byte[] getReadChunk() throws IOException {
		this.lock.lock();
		try {
			if (data != null)
				return data;
			else
				return HCServiceProxy.fetchChunk(hash, hashloc);
		} finally {
			this.lock.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.DedupChunkInterface#getLength()
	 */
	@Override
	public int getLength() {
		return length;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.DedupChunkInterface#getFilePosition()
	 */
	@Override
	public long getFilePosition() {
		return position;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.DedupChunkInterface#setLength(int)
	 */
	@Override
	public void setLength(int length) {
		this.length = length;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.DedupChunkInterface#isNewChunk()
	 */
	@Override
	public boolean isNewChunk() {
		return newChunk;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.DedupChunkInterface#setNewChunk(boolean)
	 */
	@Override
	public void setNewChunk(boolean newChunk) {
		this.newChunk = newChunk;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.DedupChunkInterface#getChunk()
	 */
	@Override
	public byte[] getChunk() throws IOException, BufferClosedException {
		this.lock.lock();
		try {
			if (data != null) {
				return data;
			} else {
				return HCServiceProxy.fetchChunk(hash, hashloc);
			}
		} finally {
			this.lock.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.DedupChunkInterface#setWritable(boolean)
	 */
	@Override
	public void setWritable(boolean writable) {
		this.writable = writable;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.DedupChunkInterface#isWritable()
	 */

	@Override
	public boolean isWritable() {
		return writable;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.DedupChunkInterface#destroy()
	 */
	@Override
	public void destroy() {

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.DedupChunkInterface#setDoop(boolean)
	 */
	@Override
	public void setDoop(boolean doop) {
		this.doop = doop;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.DedupChunkInterface#isDoop()
	 */
	@Override
	public boolean isDoop() {
		return doop;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.DedupChunkInterface#open()
	 */
	@Override
	public void open() {

	}

	@Override
	public int getBytesWritten() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public DedupFile getDedupFile() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean sync() throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int capacity() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getEndPosition() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte[] getFlushedBuffer() throws BufferClosedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isClosed() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void flush() throws BufferClosedException {
		// TODO Auto-generated method stub

	}

	@Override
	public void write(byte[] b, int pos) throws BufferClosedException,
			IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void truncate(int len) throws BufferClosedException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isDirty() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setDirty(boolean dirty) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void persist() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isPrevDoop() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setPrevDoop(boolean prevDoop) {
		// TODO Auto-generated method stub

	}

	@Override
	public byte[] getHashLoc() {
		return this.hashloc;
	}

	@Override
	public void setHashLoc(byte[] hashloc) {
		this.hashloc = hashloc;
	}

	@Override
	public boolean isBatchProcessed() {
		return false;
	}

}
