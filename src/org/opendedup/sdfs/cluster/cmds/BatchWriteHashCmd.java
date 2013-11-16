package org.opendedup.sdfs.cluster.cmds;

import java.io.IOException;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.cluster.DSEClientSocket;
import org.opendedup.sdfs.io.SparseDataChunk;

public class BatchWriteHashCmd implements IOClientCmd {
	ArrayList<SparseDataChunk> hashes;
	boolean exists = false;
	RequestOptions opts = null;

	public BatchWriteHashCmd(ArrayList<SparseDataChunk> hashes) {
		this.hashes = hashes;

	}

	@Override
	public void executeCmd(final DSEClientSocket soc) throws IOException {
		opts = new RequestOptions(ResponseMode.GET_ALL,
				Main.ClusterRSPTimeout, true);
		opts.setFlags(Message.Flag.DONT_BUNDLE);
		//opts.setFlags(Message.Flag.NO_FC);
		opts.setFlags(Message.Flag.OOB);
		try {
		byte [] ar =Util.objectToByteBuffer(hashes);
		byte[] b = new byte[1 + 4 + ar.length];
		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.put(NetworkCMDS.BATCH_HASH_EXISTS_CMD);
		buf.putInt(ar.length);
		buf.put(ar);
			List<Address> servers = soc.getServers();
			RspList<Object> lst=soc.disp.castMessage(servers,
					new Message(null, null, buf.array()), opts);
			for(SparseDataChunk ck : hashes) {
				ck.resetHashLoc();
			}
			for(Rsp<Object> rsp : lst) {
				if(rsp.hasException()) {
					SDFSLogger.getLog().error("Batch Hash Exists Exception thrown for " + rsp.getSender());
					throw rsp.getException();
				} else if(rsp.wasSuspected() | rsp.wasUnreachable()) {
					SDFSLogger.getLog().error("Batch Hash Exists Host unreachable Exception thrown for " + rsp.getSender());
				}
				else {
					if(rsp.getValue() != null) {
						SDFSLogger.getLog().debug("Batch Hash Exists completed for " +rsp.getSender() + " returned=" +rsp.getValue());
						@SuppressWarnings("unchecked")
						ArrayList<Boolean> rst = (ArrayList<Boolean>)rsp.getValue();
						byte id = soc.serverState.get(rsp.getSender()).id;
						for(int i = 0; i< rst.size(); i++) {
							boolean exists = rst.get(i);
							if(exists) {
								this.hashes.get(i).addHashLoc(id);
							}
						}
					}
				}
			}

		} catch (Throwable e) {
			SDFSLogger.getLog().error("error while getting hash", e);
			throw new IOException(e);
		}
	}

	public ArrayList<SparseDataChunk> getHashes() {
		return this.hashes;
	}
	

	@Override
	public byte getCmdID() {
		return NetworkCMDS.BATCH_HASH_EXISTS_CMD;
	}

}
