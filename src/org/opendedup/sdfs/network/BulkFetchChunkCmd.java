package org.opendedup.sdfs.network;

import java.io.ByteArrayOutputStream;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.SDFSLogger;

public class BulkFetchChunkCmd implements IOCmd {
	ArrayList<String> hashes;
	ArrayList<HashChunk> chunks;
	boolean written = false;

	public BulkFetchChunkCmd(ArrayList<String> hashes) {
		this.hashes = hashes;
	}

	@SuppressWarnings("unchecked")
	public void executeCmd(DataInputStream is, DataOutputStream os)
			throws IOException {
		
		ByteArrayOutputStream bos = null;
		bos = new ByteArrayOutputStream();
		ObjectOutputStream obj_out = new ObjectOutputStream(bos);
		obj_out.writeObject(hashes);
		byte [] sh = CompressionUtils.compressSnappy(bos.toByteArray());       
		//byte [] sh = bos.toByteArray();  
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
		byte [] us = new byte [size];
		is.readFully(us);
		SDFSLogger.getLog().debug("Recieved bulkfetch [" + us.length + "]");
		us = CompressionUtils.decompressSnappy(us);
		SDFSLogger.getLog().debug("Recieved bulkfetch uncompressed [" + us.length + "]");
		ByteArrayInputStream bin = new ByteArrayInputStream(us);
		ObjectInputStream obj_in = new ObjectInputStream(bin);
		try {
			chunks = (ArrayList<HashChunk>)obj_in.readObject();
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

	public byte getCmdID() {
		return NetworkCMDS.FETCH_CMD;
	}

}
