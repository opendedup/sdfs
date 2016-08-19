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
package org.opendedup.sdfs.network;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.filestore.HashChunk;

public class BulkWriteChunkCmd implements IOCmd {
	ArrayList<HashChunk> chunks;
	List<Boolean> response;
	boolean written = false;

	public BulkWriteChunkCmd(ArrayList<HashChunk> chunks) {
		this.chunks = chunks;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void executeCmd(DataInputStream is, DataOutputStream os)
			throws IOException {

		ByteArrayOutputStream bos = null;
		ObjectOutputStream obj_out = null;
		byte[] sh = null;
		try {
			bos = new ByteArrayOutputStream();
			obj_out = new ObjectOutputStream(bos);
			obj_out.writeObject(chunks);
			os.write(NetworkCMDS.BULK_FETCH_CMD);
			sh = bos.toByteArray();
			os.writeInt(sh.length);
			os.write(sh);
			os.flush();
		} finally {
			bos.close();
			obj_out.close();
			sh = null;
			obj_out = null;
			bos = null;
		}
		int size = is.readInt();
		if (size == -1) {
			throw new IOException("an error happened while writing");
		}
		byte[] us = new byte[size];
		is.readFully(us);
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug("Received bulkfetch [" + us.length + "]");
		// us = CompressionUtils.decompressSnappy(us);
		ByteArrayInputStream bin = new ByteArrayInputStream(us);
		ObjectInputStream obj_in = new ObjectInputStream(bin);
		try {
			response = (List<Boolean>) obj_in.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		} finally {
			us = null;
			bin.close();
			obj_in.close();
		}
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.FETCH_CMD;
	}

	@Override
	public List<Boolean> getResult() {
		return this.response;
	}

}
