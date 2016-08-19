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

import java.io.DataInputStream;

import java.io.DataOutputStream;
import java.io.IOException;

import org.opendedup.util.StringUtils;

public class FetchChunkCmd implements IOCmd {
	byte[] hash;
	byte[] chunk;
	boolean written = false;
	boolean compress = false;

	public FetchChunkCmd(byte[] hash, boolean compress) {
		this.hash = hash;
		this.compress = compress;
	}

	@Override
	public void executeCmd(DataInputStream is, DataOutputStream os)
			throws IOException {
		if (compress)
			os.write(NetworkCMDS.FETCH_COMPRESSED_CMD);
		else
			os.write(NetworkCMDS.FETCH_CMD);
		os.writeShort(hash.length);
		os.write(hash);
		os.flush();
		int size = is.readInt();
		if (size == -1) {
			throw new IOException("could not find chunk "
					+ StringUtils.getHexString(hash));
		}

		chunk = new byte[size];
		is.readFully(chunk);
		if (size == -1) {
			throw new IOException("Requested Chunk "
					+ StringUtils.getHexString(hash) + "does not exist.");
		} else if (compress) {
			throw new IOException("not implemented");
			// chunk = CompressionUtils.decompress(chunk);
		}
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.FETCH_CMD;
	}

	@Override
	public byte[] getResult() {
		return this.chunk;
	}

}
