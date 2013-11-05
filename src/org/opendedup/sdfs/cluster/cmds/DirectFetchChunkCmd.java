package org.opendedup.sdfs.cluster.cmds;

import java.io.IOException;

import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
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
		this.hashlocs = hashlocs;
		opts = new RequestOptions(ResponseMode.GET_FIRST, 0);
		// opts.setAnycasting(true);
		// opts.setFlags(Message.Flag.DONT_BUNDLE);
		// opts.setFlags(Message.Flag.OOB);
		// opts.setAnycasting(true);

	}

	@Override
	public void executeCmd(DSEClientSocket soc) throws IOException {
		int pos = 1;
		while (chunk == null) {
			HashClientPool pool = soc.getPool(hashlocs, pos);
			
			if (pool != null && hashlocs[pos] != 0) {
				HashClient cl = null;
				try {
					cl = pool.borrowObject();
					this.chunk = cl.fetchChunk(hash);

				} catch (Exception e) {
					SDFSLogger.getLog().debug("error while getting hash", e);
					
					// throw new IOException(e);
				} finally {
					if (cl != null) {
						try {
							pool.returnObject(cl);
						}catch(Exception e) {
							SDFSLogger.getLog().debug("error while getting hash and returning to pool", e);
						}
					}
				}
			}
			pos++;
			if (pos > 7)
				throw new IOException(
						"No Storage Servers available to fulfill request");
		}
	}

	public byte[] getChunk() {
		return this.chunk;
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.FETCH_CMD;
	}

}
