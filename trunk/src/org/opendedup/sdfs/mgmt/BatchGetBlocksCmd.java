package org.opendedup.sdfs.mgmt;

import java.io.ByteArrayInputStream;

import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.io.IOException;
import java.util.ArrayList;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.CompressionUtils;

public class BatchGetBlocksCmd {

	private static int MAX_BATCH_SZ = (Main.MAX_REPL_BATCH_SZ * 1024 * 1024)
			/ Main.CHUNK_LENGTH;

	public byte[] getResult(byte [] b) throws IOException, ClassNotFoundException {
		return archiveOut(b);
	}

	private synchronized byte[] archiveOut(byte [] sh) throws IOException,
			ClassNotFoundException {
		
		sh = CompressionUtils.decompressSnappy(sh);
		ObjectInputStream obj_in = new ObjectInputStream(
				new ByteArrayInputStream(sh));
		@SuppressWarnings("unchecked")
		ArrayList<byte[]> hashes = (ArrayList<byte[]>) obj_in.readObject();
		byte [] hash = null;
		if (hashes.size() > MAX_BATCH_SZ) {
			SDFSLogger.getLog().warn(
					"requested hash list to long " + hashes.size() + " > "
							+ MAX_BATCH_SZ);
			throw new IOException("requested hash list to long "
					+ hashes.size() + " > " + MAX_BATCH_SZ);
		}

		ArrayList<HashChunk> chunks = new ArrayList<HashChunk>(hashes.size());
		for (int i = 0; i < hashes.size(); i++) {
			hash = hashes.get(i);
			HashChunk dChunk = HCServiceProxy.fetchHashChunk(hash);
			chunks.add(i, dChunk);
		}
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream obj_out = new ObjectOutputStream(bos);
		obj_out.writeObject(chunks);
		byte[] b = CompressionUtils.compressSnappy(bos.toByteArray());
		return b;
	}

}
