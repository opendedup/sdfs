package org.opendedup.sdfs.cluster.cmds;

import java.io.IOException;


import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.cluster.DSEClientSocket;

public class FetchChunkCmd implements IOClientCmd {
	byte[] hash;
	byte[] chunk = null;
	RequestOptions opts = null;
	byte[] hashlocs;

	public FetchChunkCmd(byte[] hash, byte[] hashlocs) {
		this.hash = hash;
		this.hashlocs = hashlocs;
		opts = new RequestOptions(ResponseMode.GET_ALL,Main.ClusterRSPTimeout);
		opts.setFlags(Message.Flag.NO_TOTAL_ORDER);
		opts.setFlags(Message.Flag.DONT_BUNDLE);
		opts.setFlags(Message.Flag.OOB);
		opts.setAnycasting(true);
	}

	@Override
	public void executeCmd(DSEClientSocket soc) throws IOException {
		byte[] b = new byte[1 + 2 + hash.length];
		//SDFSLogger.getLog().debug("Fetching " + StringUtils.getHexString(hash));
		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.put(NetworkCMDS.FETCH_CMD);
		buf.putShort((short) hash.length);
		buf.put(hash);
		int pos = 1;
		while (chunk == null) {
			Address addr = null;
			try {
				addr = soc.getServer(hashlocs, pos);
			} catch (IOException e) {
				throw e;
			}
			ArrayList<Address> al = new ArrayList<Address>();
			al.add(addr);
			try {
				RspList<Object> lst = soc.disp.castMessage(al, new Message(
						null, null, buf.array()), opts);
				Rsp<Object> rsp = lst.get(addr);
				if(!rsp.hasException() && !rsp.wasSuspected() && !rsp.wasUnreachable()) {
					if(rsp.wasReceived())
						this.chunk = (byte[])rsp.getValue();
				}
			}catch(Exception e) {
				SDFSLogger.getLog().debug("error while getting hash", e);
			} finally {
				pos ++;
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

}