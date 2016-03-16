package org.opendedup.sdfs.mgmt;

import java.io.ByteArrayInputStream;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.CompressionUtils;

public class BatchGetPointerCmd {

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
			SDFSLogger.getLog().debug("will fetch " + hashes.size() + "blocks");

			ByteBuffer bf = ByteBuffer
					.wrap(new byte[(HashFunctionPool.hashLength + 8)
							* hashes.size()]);
			for (byte[] b : hashes) {
				long cid = HCServiceProxy.getHashesMap().get(b);
				bf.put(b);
				bf.putLong(cid);
			}
			byte[] b = CompressionUtils.compressSnappy(bf.array());
			return b;
		} catch (Throwable t) {
			SDFSLogger.getLog().error("unable to fetch blocks", t);
			throw new IOException(t);
		}
	}

}
