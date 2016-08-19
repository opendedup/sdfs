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

public class DataArchivedException extends Exception {
	long pos = -1;
	byte[] hash = null;

	/**
	 * 
	 */
	private static final long serialVersionUID = 4180645903735154438L;

	public DataArchivedException() {

	}

	public DataArchivedException(long pos, byte[] hash) {
		this.pos = pos;
		this.hash = hash;
	}

	public void setPos(long l) {
		this.pos = l;
	}

	public void setHash(byte[] h) {
		this.hash = h;
	}

	public long getPos() {
		return pos;
	}

	public byte[] getHash() {
		return hash;
	}

}
