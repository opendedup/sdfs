package org.opendedup.sdfs.cluster.cmds;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.jgroups.blocks.RequestOptions;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.cluster.DSEClientSocket;
import org.opendedup.sdfs.network.HashClient;
import org.opendedup.sdfs.network.HashClientPool;

public class DirectFetchChunkCmd implements IOClientCmd {
	byte[] hash;
	byte[] chunk = null;
	RequestOptions opts = null;
	byte[] hashlocs;

	public DirectFetchChunkCmd(byte[] hash, byte[] hashlocs) {

		this.hash = hash;
		this.hashlocs = Arrays.copyOfRange(hashlocs, 1, hashlocs.length);
		shuffleArray(this.hashlocs);
	}

	@Override
	public void executeCmd(DSEClientSocket soc) throws IOException {
		byte[] b = new byte[1 + 2 + hash.length];
		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.put(NetworkCMDS.FETCH_CMD);
		buf.putShort((short) hash.length);
		buf.put(hash);
		int pos = 0;

		while (chunk == null) {
			if (hashlocs[pos] > 0) {
				HashClientPool pool = soc.getPool(hashlocs, pos);
				if (pool != null) {
					HashClient cl = null;
					try {
						cl = (HashClient) pool.borrowObject();
						this.chunk = cl.fetchChunk(hash);
						SDFSLogger.getLog().debug(
								"fetched " + this.chunk.length);
					} catch (Exception e) {
						SDFSLogger.getLog()
								.error("error while getting hash", e);
						// throw new IOException(e);
					} finally {
						if (cl != null)
							try {
								pool.returnObject(cl);
							} catch (Exception e) {
								SDFSLogger.getLog().warn(
										"unable to return object to pool", e);
							}
					}
				} else {
					SDFSLogger.getLog().info(" pool is null at pos=" + pos);
				}
			}
			pos++;
			if (chunk == null && pos > 6) {
				String st = "";
				for (byte k : hashlocs) {
					st = st + " " + k;
				}
				throw new IOException(
						"No Storage Servers available to fulfill request ["
								+ st + "]");
			}
		}
	}

	public byte[] getChunk() {
		return this.chunk;
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.FETCH_CMD;
	}

	private static Random rnd = new Random();

	static void shuffleArray(byte[] ar) {

		for (int i = ar.length - 1; i > 1; i--) {
			int index = rnd.nextInt(i + 1);
			// Simple swap
			byte a = ar[index];
			ar[index] = ar[i];
			ar[i] = a;
		}
	}

}
