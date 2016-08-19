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

import java.util.concurrent.atomic.AtomicInteger;

import org.opendedup.collections.DataArchivedException;
import org.opendedup.hashing.Finger;

public abstract class AsyncChunkWriteActionListener {
	AtomicInteger dn = new AtomicInteger(0);
	AtomicInteger exdn = new AtomicInteger(0);
	DataArchivedException dar = null;
	int sz = 0;

	public abstract void commandException(Finger result, Throwable e);

	public abstract void commandResponse(Finger result);

	public abstract void commandArchiveException(DataArchivedException e);

	public int incrementandGetDN() {
		return dn.incrementAndGet();
	}

	public int getDN() {
		return dn.get();
	}

	public int incrementAndGetDNEX() {
		return exdn.incrementAndGet();
	}

	public int getDNEX() {
		return exdn.get();
	}

	public int getMaxSz() {
		return this.sz;
	}

	public void setMaxSize(int sz) {
		this.sz = sz;
	}

}
