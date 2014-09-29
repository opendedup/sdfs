package org.opendedup.sdfs.io;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author annesam Base class for storing byte arrays associated with dedup
 *         files into memory.
 */
public class DedupChunk implements java.io.Serializable, DedupChunkInterface {
	private static final long serialVersionUID = -5440311151699047048L;
	private int length;
	private long position;
	private boolean newChunk = false;
	private boolean writable = false;
	private int doop = 0;
	private List<HashLocPair> ar = new ArrayList<HashLocPair>();

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
	public DedupChunk(long position, int length, boolean newChunk,List<HashLocPair> ar
			) {
		
		this.length = length;
		this.position = position;
		this.newChunk = newChunk;
		this.ar = ar;
		
			
	}

	

	public byte[] getReadChunk(int start,int len) throws IOException {
		throw new IOException("not implemented");
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
	public void setDoop(int doop) {
		this.doop = doop;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.opendedup.sdfs.io.DedupChunkInterface#isDoop()
	 */
	@Override
	public int getDoop() {
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
	public int getPrevDoop() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setPrevDoop(int prevDoop) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setBatchwritten(boolean written) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<HashLocPair> getFingers() {
		// TODO Auto-generated method stub
		return this.ar;
	}

	@Override
	public boolean isBatchProcessed() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isBatchwritten() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setAR(List<HashLocPair> al) {
		this.ar = al;
		
	}

}
