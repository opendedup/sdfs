package org.opendedup.sdfs.mgmt;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.CompressionUtils;

public class BatchGetBlocksCmd {

	private static int MAX_BATCH_SZ = (Main.MAX_REPL_BATCH_SZ * 1024 * 1024)
			/ (Main.CHUNK_LENGTH/HashFunctionPool.max_hash_cluster);

	public byte[] getResult(byte[] b) throws IOException,
			ClassNotFoundException {
		return archiveOut(b);
	}

	private synchronized byte[] archiveOut(byte[] sh) throws IOException,
			ClassNotFoundException {
		try {
		sh = CompressionUtils.decompressSnappy(sh);
		ObjectInputStream obj_in = new ObjectInputStream(
				new ByteArrayInputStream(sh));
		@SuppressWarnings("unchecked")
		ArrayList<byte[]> hashes = (ArrayList<byte[]>) obj_in.readObject();
		byte[] hash = null;
		if (hashes.size() > MAX_BATCH_SZ) {
			SDFSLogger.getLog().warn(
					"requested hash list to long " + hashes.size() + " > "
							+ MAX_BATCH_SZ);
			throw new IOException("requested hash list to long "
					+ hashes.size() + " > " + MAX_BATCH_SZ);
		}
		SDFSLogger.getLog().debug("will fetch " + hashes.size() + "blocks");
		
		ArrayList<HashChunk> chunks = new ArrayList<HashChunk>(hashes.size());
		for (int i = 0; i < hashes.size(); i++) {
			hash = hashes.get(i);
			HashChunk dChunk = HCServiceProxy.fetchHashChunk(hash);
			chunks.add(i, dChunk);
			SDFSLogger.getLog().debug("fetched " + i + " blocks");
		}
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream obj_out = new ObjectOutputStream(bos);
		obj_out.writeObject(chunks);
		byte[] b = CompressionUtils.compressSnappy(bos.toByteArray());
		return b;
		}catch(Throwable t) {
			SDFSLogger.getLog().error("unable to fetch blocks", t);
			throw new IOException(t);
			
		}
	}

}
