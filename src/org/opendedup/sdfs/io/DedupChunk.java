/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.sdfs.io;

import java.io.IOException;



import java.util.TreeMap;

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
	private boolean reconstructed = false;
	private TreeMap<Integer,HashLocPair> ar = new TreeMap<Integer,HashLocPair>();

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
	public DedupChunk(long position, int length, boolean newChunk,
			TreeMap<Integer,HashLocPair> ar, boolean reconstructed) {
		this.length = length;
		this.position = position;
		this.newChunk = newChunk;
		this.ar = ar;
		this.reconstructed = reconstructed;
	}

	public byte[] getReadChunk(int start, int len) throws IOException {
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
	public boolean isClosed() {
		// TODO Auto-generated method stub
		return false;
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
	public TreeMap<Integer,HashLocPair> getFingers() {
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
	public void setAR(TreeMap<Integer,HashLocPair> al) {
		this.ar = al;

	}

	@Override
	public boolean getReconstructed() {
		return this.reconstructed;
	}

	@Override
	public void setReconstructed(boolean re) {
		this.reconstructed = re;
	}

}
