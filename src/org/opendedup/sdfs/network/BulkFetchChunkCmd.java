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

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.util.CompressionUtils;

public class BulkFetchChunkCmd implements IOCmd {
	ArrayList<String> hashes;
	ArrayList<HashChunk> chunks;
	boolean written = false;

	public BulkFetchChunkCmd(ArrayList<String> hashes) {
		this.hashes = hashes;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void executeCmd(DataInputStream is, DataOutputStream os)
			throws IOException {

		ByteArrayOutputStream bos = null;
		bos = new ByteArrayOutputStream();
		ObjectOutputStream obj_out = new ObjectOutputStream(bos);
		obj_out.writeObject(hashes);
		byte[] sh = CompressionUtils.compressSnappy(bos.toByteArray());
		// byte [] sh = bos.toByteArray();
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug("Sent bulkfetch [" + sh.length + "]");
		os.write(NetworkCMDS.BULK_FETCH_CMD);
		os.writeInt(sh.length);
		os.write(sh);
		os.flush();
		bos.close();
		obj_out.close();
		sh = null;
		obj_out = null;
		bos = null;
		int size = is.readInt();
		if (size == -1) {
			throw new IOException("One of the Requested hashes does not exist.");
		}
		byte[] us = new byte[size];
		is.readFully(us);
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug("Recieved bulkfetch [" + us.length + "]");
		us = CompressionUtils.decompressSnappy(us);
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug(
					"Recieved bulkfetch uncompressed [" + us.length + "]");
		ByteArrayInputStream bin = new ByteArrayInputStream(us);
		ObjectInputStream obj_in = new ObjectInputStream(bin);
		try {
			chunks = (ArrayList<HashChunk>) obj_in.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		} finally {
			us = null;
			bin.close();
			obj_in.close();
		}
	}

	public ArrayList<HashChunk> getChunks() {
		return this.chunks;
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.FETCH_CMD;
	}

	@Override
	public ArrayList<HashChunk> getResult() {
		return this.chunks;
	}

}
