package org.opendedup.sdfs.cluster.cmds;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RspFilter;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.cluster.DSEClientSocket;

public class HashExistsCmd implements IOClientCmd {
	byte[] hash;
	boolean exists = false;
	RequestOptions opts = null;
	byte[] resp = new byte[8];
	boolean waitforall = false;

	public HashExistsCmd(byte[] hash, boolean waitforall) {
		this.hash = hash;
		this.waitforall = waitforall;
		
	}

	@Override
	public void executeCmd(final DSEClientSocket soc) throws IOException {
		if (waitforall)
			opts = new RequestOptions(ResponseMode.GET_ALL,
					Main.ClusterRSPTimeout,false,

					new RspFilter() {

						int pos = 1;

						public boolean needMoreResponses() {
							return true;
						}

						@Override
						public boolean isAcceptable(Object response,
								Address arg1) {
							boolean rsp = ((Boolean) response).booleanValue();
							if (rsp) {
								synchronized(resp) {
									resp[0] = 1;
									resp[pos] = soc.serverState.get(arg1).id;
									pos++;
									exists = true;
								}
							}
							return rsp;
						}

					});
		else
			opts = new RequestOptions(ResponseMode.GET_ALL,
					Main.ClusterRSPTimeout, false,

					new RspFilter() {

						int pos = 1;

						public boolean needMoreResponses() {
							return !exists;
						}

						@Override
						public boolean isAcceptable(Object response,
								Address arg1) {
							boolean rsp = ((Boolean) response).booleanValue();
							if (rsp) {
								synchronized(resp) {
									resp[0] = 1;
									resp[pos] = soc.serverState.get(arg1).id;
									pos++;
									exists = rsp;
								}
							}
							return rsp;
						}

					});
		byte[] b = new byte[1 + 2 + 2 + hash.length];
		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.put(NetworkCMDS.HASH_EXISTS_CMD);
		buf.putShort((short)0);
		buf.putShort((short) hash.length);
		buf.put(hash);
		try {
			soc.disp.castMessage(soc.getServers(), new Message(null,
					null, buf.array()), opts);
			
		} catch (Exception e) {
			SDFSLogger.getLog().error("error while getting hash", e);
			throw new IOException(e);
		}
	}

	public byte[] getHash() {
		return this.hash;
	}
	
	public byte[] getResponse() {
		return this.resp;
	}

	public boolean exists() {
		return this.exists;
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.HASH_EXISTS_CMD;
	}

}
