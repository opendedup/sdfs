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

/**
 * 
 * @author annesam Lock Object for DedupFiles. Locks do not actually prevent
 *         writing to locked space but rather reserve space so that other write
 *         threads know an area is locked from writing.
 */
public class DedupFileLock {

	private DedupFileChannel channel;
	private long position;
	private long size;
	private boolean shared;
	private boolean valid;
	private String host;

	/**
	 * Instantiates a DedupFileLock
	 * 
	 * @param ch
	 *            DedupFileChannel associated with this lock.
	 * @param position
	 *            the position where the lock start position is located.
	 * @param len
	 *            the length of the lock
	 * @param shared
	 *            if the lock is shared for reading or not.
	 */
	public DedupFileLock(DedupFileChannel ch, long position, long len,
			boolean shared) {
		this.channel = ch;
		this.position = position;
		this.size = len;
		this.shared = shared;
		this.valid = true;

	}

	public String getHost() {
		return this.host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * 
	 * @return the channel associated with this lock
	 */
	public DedupFileChannel channel() {
		return this.channel;
	}

	/**
	 * 
	 * @return the size or length of the lock
	 */
	public long size() {
		return this.size;
	}

	/**
	 * 
	 * @return If the lock is shared or not
	 */
	public boolean isShared() {
		return this.shared;
	}

	/**
	 * 
	 * @return true if the lock is still valid
	 */
	public boolean isValid() {
		return this.valid;
	}

	/**
	 * sets the lock it valid = false.
	 */
	public void release() {
		this.valid = false;
	}

	/**
	 * checks to see if two locks overlap
	 * 
	 * @param pos
	 *            the proposed position of the lock
	 * @param sz
	 *            the size of the proposed lock
	 * @return true if it overlaps
	 */
	public boolean overLaps(long pos, long sz) {
		long endPos = this.position + this.size;
		long pEndPos = pos + sz;
		if (pos >= this.position && pos <= endPos)
			return true;
		if (pEndPos >= this.position && pEndPos <= endPos)
			return true;
		return false;
	}

}
