package org.opendedup.sdfs.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.opendedup.sdfs.Main;
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
		if(size != Main.CHUNK_LENGTH)
			throw new IOException("invalid chunk length " +size);
		if (size == -1) {
			chunk = new byte[0];
		} else if(size != Main.CHUNK_LENGTH)
			throw new IOException("invalid chunk length " +size);
		else {
			chunk = new byte[size];
		}
		is.readFully(chunk);
		if (size == -1) {
			throw new IOException("Requested Chunk "
					+ StringUtils.getHexString(hash) + "does not exist.");
		} else if (compress) {
			throw new IOException("not implemented");
			// chunk = CompressionUtils.decompress(chunk);
		}
	}

	public byte[] getChunk() {
		return this.chunk;
	}

	public byte getCmdID() {
		return NetworkCMDS.FETCH_CMD;
	}

}
