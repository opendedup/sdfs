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
package org.opendedup.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.opendedup.logging.SDFSLogger;

public class DirectBufPool {

	private int poolSize = 1;

	private ConcurrentLinkedQueue<ByteBuffer> passiveObjects = new ConcurrentLinkedQueue<ByteBuffer>();
	private int size = 0;
	private boolean closed = false;

	public DirectBufPool(int sz) {
		this.size = sz;
		this.populatePool();
	}

	public void populatePool() {
		for (int i = 0; i < poolSize; i++) {
			try {
				this.passiveObjects.add(this.makeObject());
			} catch (Exception e) {
				e.printStackTrace();
				SDFSLogger.getLog().fatal(
						"unable to instancial Hash Function pool", e);

			} finally {

			}
		}
	}

	public ByteBuffer borrowObject() throws IOException {
		if (this.closed)
			throw new IOException("Buf Pool closed");
		ByteBuffer hc = null;
		hc = this.passiveObjects.poll();
		if (hc == null) {
			hc = makeObject();
		}
		return hc;
	}

	public void returnObject(ByteBuffer buf) {
		if (!this.closed) {
			buf.position(0);
			this.passiveObjects.add(buf);
		} else {
			buf.clear();
			buf = null;
		}
	}

	public ByteBuffer makeObject() {
		return ByteBuffer.allocateDirect(size);
	}

	public void destroyObject(ByteBuffer buf) {
		buf.clear();
		buf = null;
	}

	public void close() {
		this.closed = true;
		while (this.passiveObjects.peek() != null) {
			this.destroyObject(this.passiveObjects.poll());
		}
	}

}
