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
package org.opendedup.collections;

import java.io.IOException;

import org.opendedup.sdfs.io.FileClosedException;

public interface DataMapInterface {

	public abstract void iterInit() throws IOException;

	public abstract long getIterPos();

	public abstract long nextKey() throws IOException, FileClosedException;

	public abstract SparseDataChunk nextValue(boolean refcount) throws IOException, FileClosedException;

	public abstract boolean isClosed();

	public abstract void put(long pos, SparseDataChunk data) throws IOException,
			FileClosedException;

	public abstract void put(long pos, SparseDataChunk data, int length)
			throws IOException, FileClosedException;

	public abstract void putIfNull(long pos, SparseDataChunk data) throws IOException,
			FileClosedException;

	public abstract void trim(long pos, int len) throws IOException,
			FileClosedException;

	public abstract void truncate(long length) throws IOException,
			FileClosedException;

	public abstract byte getVersion();

	public abstract byte[] getFree();

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#remove(long)
	 */
	public abstract void remove(long pos) throws IOException,
			FileClosedException;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.annesam.collections.AbstractMap#get(long)
	 */
	public abstract SparseDataChunk get(long pos) throws IOException,
			FileClosedException;

	public abstract void sync() throws IOException;

	public abstract void vanish(boolean refcount) throws IOException;

	public abstract void copy(String destFilePath,boolean index) throws IOException;

	public abstract long size();

	public abstract void close() throws IOException;

}