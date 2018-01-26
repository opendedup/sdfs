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
package org.opendedup.hashing;

import java.io.IOException;

import java.util.Arrays;
import java.util.List;

import org.opendedup.collections.HashtableFullException;
import org.opendedup.collections.InsertRecord;
import org.opendedup.sdfs.io.AsyncChunkWriteActionListener;
import org.opendedup.sdfs.io.WritableCacheBuffer;
import org.opendedup.sdfs.servers.HCServiceProxy;

public class Finger implements Runnable {
	private static final byte[] k = new byte[HashFunctionPool.hashLength];
	public byte[] chunk;
	public byte[] hash;
	public InsertRecord hl;
	public int start;
	public int len;
	public int ap;
	public boolean noPersist;
	public AsyncChunkWriteActionListener l;
	public int claims = -1;
	public String lookupFilter = null;
	public String uuid = null;

	public Finger(String lookupFilter, String uuid) {
		this.lookupFilter = lookupFilter;
		this.uuid = uuid;
	}

	public void run() {
		try {
			if (Arrays.equals(this.hash, k))
				this.hl = new InsertRecord(false, 1);
			this.hl = HCServiceProxy.writeChunk(this.hash, this.chunk, claims, this.lookupFilter, this.uuid);

			l.commandResponse(this);

		} catch (Throwable e) {
			l.commandException(this, e);
		}
	}

	public static class FingerPersister implements Runnable {
		public AsyncChunkWriteActionListener l;
		public List<Finger> fingers;
		public boolean dedup;

		@Override
		public void run() {
			for (Finger f : fingers) {
				try {
					f.hl = HCServiceProxy.writeChunk(f.hash, f.chunk, f.claims, f.lookupFilter, f.uuid);

					l.commandResponse(f);

				} catch (Throwable e) {
					l.commandException(f, e);
				}
			}

		}

		public void persist() throws IOException, HashtableFullException {
			for (Finger f : fingers) {
				if (Arrays.equals(f.hash, WritableCacheBuffer.bk))
					f.hl = new InsertRecord(false, 1);
				f.hl = HCServiceProxy.writeChunk(f.hash, f.chunk, f.claims, f.lookupFilter, f.uuid);

			}
		}

	}

	public void persist() {

	}
}
