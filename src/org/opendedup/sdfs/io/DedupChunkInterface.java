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

import org.opendedup.collections.DataArchivedException;

public interface DedupChunkInterface {

	/**
	 * 
	 * @return gets the lenth of the DedupChunk
	 */
	public abstract int getLength();

	public boolean getReconstructed();

	public void setReconstructed(boolean re);

	/**
	 * 
	 * @return the file position within the DedupFile
	 */
	public abstract long getFilePosition();

	public abstract byte[] getReadChunk(int start, int end) throws IOException,
			BufferClosedException, DataArchivedException;

	public TreeMap<Integer,HashLocPair> getFingers();

	/**
	 * 
	 * @param length
	 *            the length of the dedup chunk
	 */
	public abstract void setLength(int length);

	/**
	 * 
	 * @return if this is a new chunk or one retrieved from the chunk store
	 *         service
	 */
	public abstract boolean isNewChunk();

	/**
	 * 
	 * @param newChunk
	 *            sets the chunk as new
	 */
	public abstract void setNewChunk(boolean newChunk);

	/**
	 * sets the chunk as writable
	 * 
	 * @param writable
	 *            true if writable
	 */
	public abstract void setWritable(boolean writable);

	/**
	 * 
	 * @return true if the chunk is writable
	 */

	public abstract boolean isWritable();

	public abstract void destroy();

	public abstract void setDoop(int doop);

	public abstract int getDoop();

	public abstract int getBytesWritten();

	public abstract DedupFile getDedupFile();

	public abstract boolean sync() throws IOException;

	public abstract int capacity();

	public abstract long getEndPosition();

	public boolean isClosed();

	public boolean isBatchProcessed();

	public boolean isBatchwritten();

	public void setAR(TreeMap<Integer,HashLocPair> al);

	public void setBatchwritten(boolean written);

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

	public abstract void write(byte[] b, int pos) throws BufferClosedException,
			IOException, DataArchivedException;

	public abstract void truncate(int len) throws BufferClosedException;

	public abstract boolean isDirty();

	public abstract void setDirty(boolean dirty);

	public abstract String toString();

	public abstract void close() throws IOException;

	public abstract void persist();

	public abstract int getPrevDoop();

	public abstract void setPrevDoop(int prevDoop);

	public abstract int hashCode();

	void open();

}