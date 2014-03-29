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
		if(SDFSLogger.isDebug())
		SDFSLogger.getLog().debug("Received bulkfetch [" + us.length + "]");
		//us = CompressionUtils.decompressSnappy(us);
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
